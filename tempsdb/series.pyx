import shutil
import threading
import ujson
from satella.files import read_in_file

from .chunks cimport create_chunk, Chunk
from .exceptions import DoesNotExist, Corruption, InvalidState, AlreadyExists
import os

DEF METADATA_FILE_NAME = 'metadata.txt'


cdef class TimeSeries:
    """
    This is thread-safe

    :ivar last_entry_ts: timestamp of the last entry added or 0 if no entries yet (int)
    :ivar last_entry_synced: timestamp of the last synchronized entry (int)
    :ivar block_size: size of the writable block of data (int)
    :ivar path: path to the directory containing the series (str)
    :ivar descriptor_based_access: are all chunks using descriptor-based access? (bool)
    :ivar name: name of the series (str)
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
        try:
            return tpl
        finally:
            it.close()

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

    cpdef int enable_mmap(self) except -1:
        """
        Switches to mmap-based file access method for the entire series,
        and all chunks open inside.
        """
        self.descriptor_based_access = False
        cdef Chunk chunk
        with self.lock, self.open_lock:
            for chunk in self.open_chunks.values():
                chunk.switch_to_mmap_based_access()
        return 0

    def __init__(self, path: str, name: str, use_descriptor_based_access: bool = False):
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
            str metadata_s = read_in_file(os.path.join(self.path, METADATA_FILE_NAME),
                                         'utf-8', 'invalid json')
            dict metadata
            str filename
            list files = os.listdir(self.path)
            unsigned long long last_chunk_name

        try:
            metadata = ujson.loads(metadata_s)      # raises ValueError
            # raises KeyError
            self.block_size = metadata['block_size']
            self.max_entries_per_chunk = metadata['max_entries_per_chunk']
            self.last_entry_synced = metadata['last_entry_synced']
            self.page_size = metadata['page_size']
        except ValueError:
            raise Corruption('Corrupted series')
        except KeyError:
            raise Corruption('Could not read metadata item')

        self.open_chunks = {}       # tp.Dict[int, Chunk]

        if not len(files):
            raise Corruption('Empty directory!')
        elif len(files) == 1:
            # empty series
            self.last_chunk = None
            self.chunks = []
            self.last_entry_ts = 0
        else:
            self.chunks = []        # type: tp.List[int] # sorted by ASC
            for filename in files:
                if filename == METADATA_FILE_NAME:
                    continue
                try:
                    self.chunks.append(int(filename))
                except ValueError:
                    raise Corruption('Detected invalid file "%s"' % (filename, ))
            self.chunks.sort()

            last_chunk_name = self.chunks[-1]
            self.last_chunk = self.open_chunk(last_chunk_name)
            self.last_entry_ts = self.last_chunk.max_ts

    cdef void decref_chunk(self, unsigned long long name):
        self.refs_chunks[name] -= 1

    cdef void incref_chunk(self, unsigned long long name):
        if name not in self.refs_chunks:
            self.refs_chunks[name] = 1
        else:
            self.refs_chunks[name] += 1

    cdef Chunk open_chunk(self, unsigned long long name):
        """
        Opens a provided chunk.
        
        Acquires a reference to the chunk.
        
        :param name: name of the chunk
        :return: chunk
        :raises DoesNotExist: chunk not found
        :raises InvalidState: resource closed
        """
        if self.closed:
            raise InvalidState('Series is closed')
        if name not in self.chunks:
            raise DoesNotExist('Invalid chunk!')
        cdef Chunk chunk
        with self.open_lock:
            if name not in self.open_chunks:
                self.open_chunks[name] = chunk = Chunk(self,
                                                       os.path.join(self.path, str(name)),
                                                       self.page_size,
                                                       use_descriptor_access=self.descriptor_based_access)
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
                while len(self.chunks) >= 2 and timestamp > self.chunks[1]:
                    chunk_to_delete = self.chunks[0]
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
        return 0

    cpdef void close(self):
        """
        Close the series.
        
        No further operations can be executed on it afterwards.
        """
        if self.closed:
            return
        cdef:
            Chunk chunk
            list open_chunks
        open_chunks = list(self.open_chunks.values())
        for chunk in open_chunks:
            chunk.close()
        if self.mpm is not None:
            self.mpm.cancel()
            self.mpm = None
        self.closed = True

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
            if self.chunks[mid] < timestamp:
                lo = mid+1
            else:
                hi = mid

        try:
            if self.chunks[lo] == timestamp:
                return lo
            else:
                return lo-1
        except IndexError:
            return len(self.chunks)-1

    cpdef Iterator iterate_range(self, unsigned long long start, unsigned long long stop):
        """
        Return an iterator through collected data with given timestamps.
        
        :param start: timestamp to start at
        :param stop: timestamp to stop at
        :return: an iterator with the data
        :raises ValueError: start larger than stop
        """
        if self.last_chunk is None:
           return Iterator(self, 0, 0, [])

        if start > stop:
            raise ValueError('start larger than stop')
        if start < self.chunks[0]:
            start = self.chunks[0]
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
            chunks.append(self.open_chunk(self.chunks[chunk_index]))
        return Iterator(self, start, stop, chunks)

    cpdef int mark_synced_up_to(self, unsigned long long timestamp) except -1:
        """
        Mark the series as synced up to particular timestamp
        
        :param timestamp: timestamp of the last synced entry
        """
        self.last_entry_synced = timestamp
        self.sync_metadata()
        return 0

    cdef int sync_metadata(self) except -1:
        with self.lock, open(os.path.join(self.path, METADATA_FILE_NAME), 'w') as f_out:
            ujson.dump(self.get_metadata(), f_out)

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
        return {
                'block_size': self.block_size,
                'max_entries_per_chunk': self.max_entries_per_chunk,
                'last_entry_synced': self.last_entry_synced,
                'page_size': self.page_size
            }

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
        Close all superficially opened chunks.
        """
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
                elif not self.refs_chunks.get(chunk_name, 0):
                    self.open_chunks[chunk_name].close()
                    try:
                        del self.refs_chunks[chunk_name]
                    except KeyError:
                        pass
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
                                               descriptor_based_access=self.descriptor_based_access)
                self.open_chunks[timestamp] = self.last_chunk
                self.incref_chunk(timestamp)
                self.chunks.append(timestamp)
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

cpdef TimeSeries create_series(str path, str name, unsigned int block_size,
                               int max_entries_per_chunk, int page_size=4096,
                               bint use_descriptor_based_access=False):
    if os.path.exists(path):
        raise AlreadyExists('This series already exists!')

    os.mkdir(path)
    with open(os.path.join(path, METADATA_FILE_NAME), 'w') as f_out:
        ujson.dump({
            'block_size': block_size,
            'max_entries_per_chunk': max_entries_per_chunk,
            'last_entry_synced': 0,
            'page_size': page_size
            }, f_out
        )
    return TimeSeries(path, name)
