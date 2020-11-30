import itertools
import shutil
import threading
import time
import ujson
from satella.files import read_in_file

from .chunks cimport create_chunk, Chunk
from .database cimport Database
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
    """
    def __init__(self, path: str):
        self.mpm = None
        self.lock = threading.Lock()
        self.open_lock = threading.Lock()
        self.closed = False

        self.path = path

        if not os.path.isdir(self.path):
            raise DoesNotExist('Chosen time series does not exist')

        cdef:
            str metadata_s = read_in_file(os.path.join(self.path, METADATA_FILE_NAME),
                                         'utf-8', 'invalid json')
            dict metadata
            list files = os.listdir(self.path)
            set files_s = set(files)
            str chunk
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

        files_s.remove('metadata.txt')
        if not files_s:
            self.last_chunk = None
            self.chunks = []
            self.last_entry_ts = 0
        else:
            self.chunks = []        # type: tp.List[int] # sorted by ASC
            for chunk in files:
                try:
                    self.chunks.append(int(chunk))
                except ValueError:
                    raise Corruption('Detected invalid file "%s"' % (chunk, ))
            self.chunks.sort()
            self.last_chunk = Chunk(self, os.path.join(self.path, str(max(self.chunks))))
            self.open_chunks[self.last_chunk.min_ts] = self.last_chunk
            self.last_entry_ts = self.last_chunk.max_ts

    cpdef Chunk open_chunk(self, unsigned long long name):
        """
        Opens a provided chunk
        
        :param name: name of the chunk
        :type name: int
        :return: chunk
        :rtype: Chunk
        :raises DoesNotExist: chunk not found
        :raises InvalidState: resource closed
        """
        if self.closed:
            raise InvalidState('Series is closed')
        if name not in self.chunks:
            raise DoesNotExist('Invalid chunk!')
        with self.open_lock:
            if name not in self.open_chunks:
                self.open_chunks[name] = Chunk(self,
                                               os.path.join(self.path, str(name)),
                                               self.page_size)
        return self.open_chunks[name]

    cpdef void close(self):
        """
        Close the series.
        
        No further operations can be executed on it afterwards.
        """
        if self.closed:
            return
        cdef Chunk chunk
        for chunk in self.data_in_memory.values():
            chunk.close()
        if self.mpm is not None:
            self.mpm.cancel()
            self.mpm = None
        self.closed = True

    cpdef unsigned int get_index_of_chunk_for(self, unsigned long long timestamp):
        """
        Return the index of chunk that should have given timestamp
        
        :param timestamp: timestamp to check, larger than first timestamp,
            smaller or equal to current timestamp
        :type timestamp: int
        :return: name of the starting chunk
        :rtype: int
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

    cpdef object iterate_range(self, unsigned long long start, unsigned long long stop):
        """
        Return an iterator through collected data with given timestamps.
        
        :param start: timestamp to start at
        :type start: int
        :param stop: timestamp to stop at
        :type stop: int
        :return: an iterator with the data
        :rtype: tp.Iterator[tp.Tuple[int, bytes]]
        :raises ValueError: start larger than stop
        """
        if self.last_chunk is None:
            return iter([])
        if start > stop:
            raise ValueError('start larger than stop')
        if start < self.chunks[0]:
            start = self.chunks[0]
        if stop > self.last_entry_ts:
            stop = self.last_entry_ts

        cdef:
            unsigned int ch_start = self.get_index_of_chunk_for(start)
            unsigned int ch_stop = self.get_index_of_chunk_for(stop)
            list iterator = []
            bint is_first
            bint is_last
            unsigned int chunk_index
            Chunk chunk

        for chunk_index in range(ch_start, ch_stop+1):
            chunk = self.open_chunk(self.chunks[chunk_index])
            is_first = chunk_index == ch_start
            is_last = chunk_index == ch_stop
            if is_first and is_last:
                return chunk.iterate_indices(chunk.find_left(start), chunk.find_right(stop))
            elif is_first:
                iterator.append(chunk.iterate_indices(chunk.find_left(start), chunk.entries))
            elif is_last:
                iterator.append(chunk.iterate_indices(0, chunk.find_right(stop)))
            else:
                iterator.append(chunk.iterate_indices(0, chunk.entries))
        return itertools.chain(*iterator)

    cpdef int mark_synced_up_to(self, unsigned long long timestamp) except -1:
        """
        Mark the series as synced up to particular timestamp
        
        :param timestamp: timestamp of the last synced entry
        :type timestamp: int
        """
        self.last_entry_synced = timestamp
        self.sync()
        return 0

    cpdef int sync(self) except -1:
        """
        Synchronize the data kept in the memory with these kept on disk
        
        :raises InvalidState: the resource is closed
        """
        if self.closed:
            raise InvalidState('series is closed')

        with self.lock, open(os.path.join(self.path, METADATA_FILE_NAME), 'w') as f_out:
            ujson.dump(self._get_metadata(), f_out)

        if self.last_chunk:
            self.last_chunk.sync()

        return 0

    cdef dict _get_metadata(self):
        return {
                'block_size': self.block_size,
                'max_entries_per_chunk': self.max_entries_per_chunk,
                'last_entry_synced': self.last_entry_synced,
                'page_size': self.page_size
            }

    cpdef void register_memory_pressure_manager(self, object mpm):
        """
        Register a memory pressure manager.
        
        This registers :meth:`~tempsdb.series.TimeSeries.close_chunks` as remaining in severity
        to be called each 30 minutes.
        """
        self.mpm = mpm.register_on_remaining_in_severity(1, 30)(self.close_chunks)

    cpdef int close_chunks(self) except -1:
        """
        Close all superficially opened chunks
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
                if chunk_name != last_chunk_name:
                    continue
                else:
                    self.open_chunks[chunk_name].close()
                    del self.open_chunks[chunk_name]
        return 0

    cpdef int append(self, unsigned long long timestamp, bytes data) except -1:
        """
        Append an entry.
        
        :param timestamp: timestamp, must be larger than current last_entry_ts
        :type timestamp: int
        :param data: data to write
        :type data: bytes
        :raises ValueError: Timestamp not larger than previous timestamp or invalid block size
        :raises InvalidState: the resource is closed
        """
        if self.closed:
            raise InvalidState('series is closed')
        if len(data) != self.block_size:
            raise ValueError('Invalid block size, was %s should be %s' % (len(data), self.block_size))
        if timestamp <= self.last_entry_ts:
            raise ValueError('Timestamp not larger than previous timestamp')

        with self.lock, self.open_lock:
            # If this is indeed our first chunk, or we've exceeded the limit of entries per chunk
            if self.last_chunk is None or self.last_chunk.length() >= self.max_entries_per_chunk:
                # Create a next chunk
                self.last_chunk = create_chunk(self, os.path.join(self.path, str(timestamp)),
                                               timestamp, data, self.page_size)
                self.open_chunks[timestamp] = self.last_chunk
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


cpdef TimeSeries create_series(str path, unsigned int block_size,
                               int max_entries_per_chunk, int page_size=4096):
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
    return TimeSeries(path)
