import resource
import os
import typing as tp
import shutil
import threading
import warnings

from .chunks.base cimport Chunk
from .chunks.normal cimport NormalChunk
from .chunks.direct cimport DirectChunk
from .chunks.maker cimport create_chunk
from .exceptions import DoesNotExist, Corruption, InvalidState, AlreadyExists
from .metadata cimport read_meta_at, write_meta_at


cdef set metadata_file_names = {'metadata.txt', 'metadata.minijson'}


cdef class TimeSeries:
    """
    A single time series. This maps each timestamp (unsigned long long) to a block of data
    of length block_size.

    When you're done with this, please call
    :meth:`~tempsdb.series.TimeSeries.close`.

    If you forget to, the destructor will do that instead, and a warning will be emitted.

    :ivar last_entry_ts: timestamp of the last entry added or 0 if no entries yet (int)
    :ivar last_entry_synced: timestamp of the last synchronized entry (int)
    :ivar block_size: size of the writable block of data (int)
    :ivar path: path to the directory containing the series (str)
    :ivar descriptor_based_access: are all chunks using descriptor-based access? (bool)
    :ivar name: name of the series (str)
    :ivar metadata: extra data (tp.Optional[dict])
    """
    cpdef tuple get_current_value(self):
        """
        Return latest value of this series
                        
        :return: tuple of (timestamp, value)
        :rtype: tp.Tuple[int, bytes]
        :raises ValueError: series has no data
        """
        if self.last_chunk is None:
            raise ValueError('No data in series')
        cdef:
            Iterator it = self.iterate_range(self.last_chunk.max_ts, self.last_chunk.max_ts)
            tuple tpl = it.next_item()
        it.close()

        if tpl is None:
            raise ValueError('Series is empty!')

        return tpl

    cpdef int disable_mmap(self) except -1:
        """
        Switches to descriptor-based file access method for the entire series,
        and all chunks open inside.
        """
        self.descriptor_based_access = True
        cdef Chunk chunk
        with self.lock, self.open_lock:
            for chunk in self.open_chunks.values():
                chunk.switch_to_descriptor_based_access()
        return 0

    cpdef int set_metadata(self, dict new_meta) except -1:
        """
        Set a new value for the :attr:`~tempsdb.series.TimeSeries.metadata` property.
        
        This writes the disk.
        
        :param new_meta: new value of metadata property
        """
        self.metadata = new_meta
        self.sync_metadata()
        return 0

    cpdef int enable_mmap(self) except -1:
        """
        Switches to mmap-based file access method for the entire series,
        and all chunks open inside.
        
        This will try to enable mmap on every chunk, but if mmap fails due to recoverable
        errors, it will remain in descriptor-based mode.
        
        :raises Corruption: mmap failed due to an irrecoverable error
        """
        self.descriptor_based_access = False
        cdef Chunk chunk
        with self.lock, self.open_lock:
            for chunk in self.open_chunks.values():
                chunk.switch_to_mmap_based_access()
        return 0

    def __init__(self, str path, str name, bint use_descriptor_based_access = False):
        self.descriptor_based_access = use_descriptor_based_access
        self.mpm = None
        self.name = name
        self.lock = threading.RLock()
        self.open_lock = threading.RLock()
        self.refs_chunks = {}
        self.closed = False

        self.path = path

        if not os.path.isdir(self.path):
            raise DoesNotExist('Chosen time series does not exist')

        cdef:
            dict metadata = read_meta_at(self.path)
            str filename
            list files = os.listdir(self.path)
            unsigned long long last_chunk_name
            bint is_direct
            bint is_gzip
            bytes meta_d

        try:
            self.block_size = metadata['block_size']
            self.max_entries_per_chunk = metadata['max_entries_per_chunk']
            self.last_entry_synced = metadata['last_entry_synced']
            self.page_size = metadata['page_size']
            self.metadata = metadata.get('metadata')
            self.gzip_level = metadata.get('gzip_level', 0)
        except ValueError:
            raise Corruption('Corrupted series')
        except (OSError, ValueError) as e:
            raise Corruption('Corrupted series: %s' % (e, ))
        except KeyError:
            raise Corruption('Could not read metadata item')

        self.open_chunks = {}       # tp.Dict[int, Chunk]
        self.chunks = []            # type: tp.List[tp.Tuple[int, bool, bool]] # sorted by ASC
                                    #: timestamp, is_direct, is_gzip

        if not len(files):
            raise Corruption('Empty directory!')
        elif len(files) == 1:
            # empty series
            self.last_chunk = None
            self.last_entry_ts = 0
        else:
            for filename in files:
                if filename in metadata_file_names:
                    continue
                is_gzip = filename.endswith('.gz')
                if is_gzip:
                    filename = filename.replace('.gz', '')
                is_direct = filename.endswith('.direct')
                if is_direct:
                    filename = filename.replace('.direct', '')
                is_direct |= is_gzip
                try:
                    self.chunks.append((int(filename), is_direct, is_gzip))
                except ValueError:
                    raise Corruption('Detected invalid file "%s"' % (filename, ))
            self.chunks.sort()

            last_chunk_name, is_direct, is_gzip = self.chunks[-1]
            self.last_chunk = self.open_chunk(last_chunk_name, is_direct, is_gzip)
            self.last_entry_ts = self.last_chunk.max_ts

    cdef void decref_chunk(self, unsigned long long name):
        self.refs_chunks[name] -= 1

    cdef void incref_chunk(self, unsigned long long name):
        if name not in self.refs_chunks:
            self.refs_chunks[name] = 1
        else:
            self.refs_chunks[name] += 1

    cdef Chunk open_chunk(self, unsigned long long name, bint is_direct, bint is_gzip):
        """
        Opens a provided chunk.
        
        Acquires a reference to the chunk.
        
        :param name: name of the chunk
        :param is_direct: is this a direct chunk?
        :param is_gzip: is this a gzipped chunk?
        :return: chunk
        :raises DoesNotExist: chunk not found
        :raises InvalidState: resource closed
        :raises ValueError: chunk was gzipped but not direct
        """
        if self.closed:
            raise InvalidState('Series is closed')
        if name not in (v[0] for v in self.chunks):
            raise DoesNotExist('Invalid chunk')
        if is_gzip and not is_direct:
            raise ValueError('Chunk that is gzipped must be direct')
        cdef Chunk chunk
        with self.open_lock:
            if name not in self.open_chunks:
                if is_direct:
                    chunk = DirectChunk(self,
                                        os.path.join(self.path, str(name)),
                                        self.page_size,
                                        use_descriptor_access=True,
                                        gzip_compression_level=self.gzip_level if is_gzip else 0)
                else:
                    chunk = NormalChunk(self,
                                  os.path.join(self.path, str(name)),
                                  self.page_size,
                                  use_descriptor_access=self.descriptor_based_access)
                self.open_chunks[name] = chunk
            else:
                chunk = self.open_chunks[name]
            self.incref_chunk(name)
        return chunk

    cpdef int trim(self, unsigned long long timestamp) except -1:
        """
        Delete all entries earlier than timestamp.
        
        Note that this will drop entire chunks, so it may be possible that some entries will linger
        on. This will not delete currently opened chunks!
        
        :param timestamp: timestamp to delete entries earlier than
        """
        if len(self.chunks) == 1:
            return 0
        cdef:
            unsigned long long chunk_to_delete
            int refs
        try:
            with self.open_lock:
                while len(self.chunks) >= 2 and timestamp > self.chunks[1][0]:
                    chunk_to_delete = self.chunks[0][0]
                    if chunk_to_delete in self.open_chunks:
                        refs = self.refs_chunks.get(chunk_to_delete, 0)
                        if not refs:
                            self.open_chunks[chunk_to_delete].delete()
                        else:
                            # I would delete it, but it's open...
                            return 0
                    else:
                        os.unlink(os.path.join(self.path, str(chunk_to_delete)))
                    del self.chunks[0]
                else:
                    return 0
        except IndexError:
            return 0
        if len(self.chunks) > 1:
            try:
                with self.open_lock:
                    while len(self.chunks) >= 2 and timestamp > self.chunks[1]:
                        chunk_to_delete = self.chunks[0]
                        if chunk_to_delete in self.open_chunks:
                            refs = self.get_references_for(chunk_to_delete)
                            if not refs:
                                self.open_chunks[chunk_to_delete].delete()
                            else:
                                # I would delete it, but it's open...
                                break
                        else:
                            os.unlink(os.path.join(self.path, str(chunk_to_delete)))
                        del self.chunks[0]
            except IndexError:
                pass
        return 0

    cpdef int close(self) except -1:
        """
        Close the series.
        
        No further operations can be executed on it afterwards.
        """
        cdef:
            Chunk chunk
            list open_chunks
        if self.closed:
            return 0
        open_chunks = list(self.open_chunks.values())
        for chunk in open_chunks:
            chunk.close(True)
        if self.mpm is not None:
            self.mpm.cancel()
            self.mpm = None
        self.closed = True
        return 0

    cdef unsigned int get_index_of_chunk_for(self, unsigned long long timestamp):
        """
        Return the index of chunk that should have given timestamp
        
        :param timestamp: timestamp to check, larger than first timestamp,
            smaller or equal to current timestamp
        :return: name of the starting chunk
        """
        cdef:
            unsigned int lo = 0
            unsigned int hi = len(self.chunks)
            unsigned int mid
        while lo < hi:
            mid = (lo+hi)//2
            if self.chunks[mid][0] < timestamp:
                lo = mid+1
            else:
                hi = mid

        try:
            if self.chunks[lo][0] == timestamp:
                return lo
            else:
                return lo-1
        except IndexError:
            return len(self.chunks)-1

    cpdef Iterator iterate_range(self, unsigned long long start, unsigned long long stop,
                                 bint direct_bytes=True):
        """
        Return an iterator through collected data with given timestamps.
        
        :param start: timestamp to start at
        :param stop: timestamp to stop at
        :param direct_bytes: for compatibility with VarlenSeries. Ignored.
        :return: an iterator with the data
        :raises ValueError: start larger than stop
        """
        if self.last_chunk is None:
           return Iterator(self, 0, 0, [])

        if start > stop:
            raise ValueError('start larger than stop')
        if start < self.chunks[0][0]:
            start = self.chunks[0][0]
        if stop > self.last_entry_ts:
            stop = self.last_entry_ts

        cdef:
            unsigned int ch_start = self.get_index_of_chunk_for(start)
            unsigned int ch_stop = self.get_index_of_chunk_for(stop)
            list chunks = []
            bint is_first
            bint is_last
            unsigned int chunk_index
            Chunk chunk

        for chunk_index in range(ch_start, ch_stop+1):
            ts, is_direct, is_gzip = self.chunks[chunk_index]
            chunks.append(self.open_chunk(ts, is_direct, is_gzip))
        return Iterator(self, start, stop, chunks)

    cpdef int mark_synced_up_to(self, unsigned long long timestamp) except -1:
        """
        Mark the series as synced up to particular timestamp.
        
        This will additionally sync the metadata.
        
        :param timestamp: timestamp of the last synced entry
        """
        self.last_entry_synced = timestamp
        self.sync_metadata()
        return 0

    cdef int sync_metadata(self) except -1:
        """
        Write the metadata to disk
        """
        return write_meta_at(self.path, self.get_metadata())

    cpdef int sync(self) except -1:
        """
        Synchronize the data kept in the memory with these kept on disk
        
        :raises InvalidState: the resource is closed
        """
        if self.closed:
            raise InvalidState('series is closed')

        self.sync_metadata()

        if self.last_chunk is not None:
            self.last_chunk.sync()

        return 0

    cdef dict get_metadata(self):
        cdef dict meta = {
                'block_size': self.block_size,
                'max_entries_per_chunk': self.max_entries_per_chunk,
                'last_entry_synced': self.last_entry_synced,
                'page_size': self.page_size
            }
        if self.metadata is not None:
            meta['metadata'] = self.metadata
        return meta

    cdef void register_memory_pressure_manager(self, object mpm):
        """
        Register a memory pressure manager.
        
        This registers :meth:`~tempsdb.series.TimeSeries.close_chunks` as remaining in severity
        to be called each 30 seconds.
        
        No op if already closed
        """
        if self.closed:
            return
        self.mpm = mpm.register_on_remaining_in_severity(1, 30)(self.close_chunks)

    cpdef int close_chunks(self) except -1:
        """
        Close all chunks opened by read requests that are not referred to anymore.
        
        No-op if closed.
        """
        if self.closed:
            return 0
        if self.last_chunk is None:
            return 0
        if len(self.chunks) == 1:
            return 0
        cdef:
            unsigned long long chunk_name
            list chunks = list(self.open_chunks.keys())
            unsigned long long last_chunk_name = self.last_chunk.name()

        with self.open_lock:
            for chunk_name in chunks:
                if chunk_name == last_chunk_name:
                    continue
                elif not self.get_references_for(chunk_name):
                    self.open_chunks[chunk_name].close()
                    try:
                        del self.refs_chunks[chunk_name]
                    except KeyError:
                        pass
        return 0

    cpdef int append_padded(self, unsigned long long timestamp, bytes data) except -1:
        """
        Same as :meth:`~tempsdb.series.TimeSeries.append` but will accept data shorter
        than block_size.
        
        It will be padded with zeros.

        :param timestamp: timestamp, must be larger than current last_entry_ts
        :param data: data to write
        :raises ValueError: Timestamp not larger than previous timestamp or invalid block size
        :raises InvalidState: the resource is closed
        """
        cdef int data_len = len(data)
        if data_len > self.block_size:
            raise ValueError('Data too long')
        data = data + b'\x00'*(self.block_size - data_len)
        self.append(timestamp, data)
        return 0

    cpdef int append(self, unsigned long long timestamp, bytes data) except -1:
        """
        Append an entry.
        
        :param timestamp: timestamp, must be larger than current last_entry_ts
        :param data: data to write
        :raises ValueError: Timestamp not larger than previous timestamp or invalid block size
        :raises InvalidState: the resource is closed
        """
        if self.closed:
            raise InvalidState('series is closed')
        if len(data) != self.block_size:
            raise ValueError('Invalid block size, was %s should be %s' % (len(data), self.block_size))
        if timestamp <= self.last_entry_ts and self.last_entry_ts:
            raise ValueError('Timestamp not larger than previous timestamp')

        with self.lock, self.open_lock:
            # If this is indeed our first chunk, or we've exceeded the limit of entries per chunk
            if self.last_chunk is None or self.last_chunk.length() >= self.max_entries_per_chunk:
                # Create a next chunk
                if self.last_chunk is not None:
                    self.decref_chunk(self.last_chunk.name())
                self.last_chunk = create_chunk(self, os.path.join(self.path, str(timestamp)),
                                               timestamp, data, self.page_size,
                                               descriptor_based_access=self.descriptor_based_access,
                                               use_direct_mode=bool(self.gzip_level),
                                               gzip_compression_level=self.gzip_level)
                self.open_chunks[timestamp] = self.last_chunk
                self.incref_chunk(timestamp)
                self.chunks.append((timestamp, bool(self.gzip_level), bool(self.gzip_level)))
            else:
                self.last_chunk.append(timestamp, data)
            self.last_entry_ts = timestamp

        return 0

    cpdef int delete(self) except -1:
        """
        Erase this series from the disk. Series must be opened to do that.
        
        :raises InvalidState: series is not opened
        """
        if self.closed:
            raise InvalidState('series is closed')
        self.close()
        shutil.rmtree(self.path)
        return 0

    cpdef unsigned long open_chunks_mmap_size(self):
        """
        Calculate how much RAM does the mmaped space take
        
        :return: how much RAM, in bytes, do the opened chunks consume?
        """
        cdef:
            unsigned long ram = 0
            Chunk chunk
        for chunk in self.open_chunks.values():
            ram += chunk.get_mmap_size()
        return ram

    def __del__(self):
        if not self.closed:
            warnings.warn('You forgot to close TimeSeries. Please explicitly close it when you '
                          'are done.')
            self.close()


cpdef TimeSeries create_series(str path, str name, unsigned int block_size,
                               int max_entries_per_chunk, int page_size=0,
                               bint use_descriptor_based_access=False,
                               int gzip_level=0):
    if not page_size:
        page_size = resource.getpagesize()
    if os.path.exists(path):
        raise AlreadyExists('This series already exists!')
    os.mkdir(path)
    cdef dict meta = {
            'block_size': block_size,
            'max_entries_per_chunk': max_entries_per_chunk,
            'last_entry_synced': 0,
            'page_size': page_size
    }
    if gzip_level:
        meta['gzip_level'] = gzip_level
    write_meta_at(path, meta)
    return TimeSeries(path, name,
                      use_descriptor_based_access=use_descriptor_based_access)
