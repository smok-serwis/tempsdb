import threading
import time
import ujson
from satella.files import read_in_file

from .chunks cimport create_chunk, Chunk
from .database cimport Database
from .exceptions import DoesNotExist, Corruption, InvalidState
import os

DEF METADATA_FILE_NAME = 'metadata.txt'

cdef class TimeSeries:
    """
    This is thread-safe

    :ivar last_entry_ts: timestamp of the last entry added (int)
    :ivar last_entry_synced: timestamp of the last synchronized entry (int)
    :ivar block_size: size of the writable block of data
    """
    def __init__(self, parent: Database, name: str):
        self.lock = threading.Lock()
        self.fopen_lock = threading.Lock()
        self.parent = parent
        self.name = name
        self.closed = False

        if not os.path.isdir(self.parent.path, name):
            raise DoesNotExist('Chosen time series does not exist')

        self.path = os.path.join(self.parent.path, self.name)

        cdef:
            str metadata_s = read_in_file(os.path.join(self.path, METADATA_FILE_NAME),
                                         'utf-8', 'invalid json')
            dict metadata
            list files = os.path.listdir(self.path)
            set files_s = set(files)
            str chunk
        try:
            metadata = ujson.loads(metadata_s)
        except ValueError:
            raise Corruption('Corrupted series')

        files_s.remove('metadata.txt')
        self.chunks = []        # type: tp.List[int] # sorted by ASC
        for chunk in files:
            try:
                self.chunks.append(int(chunk))
            except ValueError:
                raise Corruption('Detected invalid file "%s"' % (chunk, ))

        self.chunks.sort()
        try:
            self.last_entry_ts = metadata['last_entry_ts']
            self.block_size = metadata['block_size']
            self.max_entries_per_block = metadata['max_entries_per_block']
            self.last_entry_synced = metadata['last_entry_synced']
            self.interval_between_synces = metadata['interval_between_synces']
        except KeyError:
            raise Corruption('Could not read metadata item')

        self.data_in_memory = []
        self.open_chunks = {}       # tp.Dict[int, Chunk]
        self.last_synced = time.monotonic()
        self.last_chunk = Chunk(os.path.join(self.path, str(max(self.chunks))))

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
        with self.fopen_lock:
            if name not in self.open_chunks:
                self.open_chunks[name] = Chunk(os.path.join(self.path, str(name)))
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
        self.closed = True

    cpdef int mark_synced_up_to(self, unsigned long long timestamp) except -1:
        """
        Mark the series as synced up to particular timestamp
        
        :param timestamp: timestamp of the last synced entry
        :type timestamp: int
        """
        self.last_entry_synced = timestamp
        self._sync_metadata()
        return 0

    cpdef int sync(self) except -1:
        """
        Synchronize the data kept in the memory with these kept on disk
        
        :raises InvalidState: the resource is closed
        """
        if self.closed:
            raise InvalidState('series is closed')
        cdef:
            unsigned long long min_ts = self.data_in_memory[0][0]
            str path = os.path.join(self.path, str(min_ts))
        with self.lock:
            self.last_synced = time.monotonic()
            if not self.data_in_memory:
                return 0

            chunk = create_chunk(path, self.data_in_memory)
            self.chunks.append(chunk.min_ts)
            self.data_in_memory = []
            self._sync_metadata()
        return 0

    cdef dict _get_metadata(self):
        return {
                'last_entry_ts': self.last_entry_ts,
                'block_size': self.block_size,
                'max_entries_per_block': self.max_entries_per_block,
                'last_entry_synced': self.last_entry_synced,
                'interval_between_synces': self.interval_between_synces
            }

    cpdef int _sync_metadata(self) except -1:
        with open(os.path.join(self.path, METADATA_FILE_NAME), 'w') as f_out:
            ujson.dump(self._get_metadata(), f_out)
        return 0

    cpdef int try_sync(self) except -1:
        """
        Check if synchronization is necessary, and if so, perform it.
        
        Prefer this to :meth:`~tempsdb.series.Series.sync`
        """
        if len(self.data_in_memory) == self.max_entries_per_block or \
            time.monotonic() - self.last_synced > self.interval_between_synces:
            self.sync()
        return 0

    cpdef int put(self, unsigned long long timestamp, bytes data) except -1:
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
            raise ValueError('Invalid block size')
        if timestamp <= self.last_entry_ts:
            raise ValueError('Timestamp not larger than previous timestamp')

        with self.lock:
            if len(self.last_chunk) >= self.max_entries_per_block:
                self.last_chunk.close()
                self.last_chunk = create_chunk(os.path.join(self.path, str(timestamp)),
                                               [(timestamp, data)])
            else:
                self.last_chunk.put(timestamp, data)

            self.last_entry_ts = timestamp

        return 0
