import os
import threading
import typing as tp
import struct
import mmap
from .exceptions import Corruption, InvalidState, AlreadyExists
from .series cimport TimeSeries

DEF HEADER_SIZE = 4
DEF TIMESTAMP_SIZE = 8
DEF FOOTER_SIZE = 4
STRUCT_Q = struct.Struct('<Q')
STRUCT_L = struct.Struct('<L')
STRUCT_LQ = struct.Struct('<LQ')


cdef class Chunk:
    """
    Represents a single chunk of time series.

    This also implements an iterator interface, and will iterate with tp.Tuple[int, bytes],
    as well as a sequence protocol

    :param parent: parent time series
    :type parent: tp.Optional[TimeSeries]
    :param path: path to the chunk file
    :type path: str

    :ivar path: path to the chunk (str)
    :ivar min_ts: timestamp of the first entry stored (int)
    :ivar max_ts: timestamp of the last entry stored (int)
    :ivar block_size: size of the data entries (int)
    :ivar entries: amount of entries in this chunk (int)
    :ivar writable: is this chunk writable (bool)
    """
    def __init__(self, parent: tp.Optional[TimeSeries], path: str, page_size: int):
        cdef bytes b
        self.file_size = os.path.getsize(path)
        self.page_size = page_size
        self.parent = parent
        self.closed = False
        self.path = path
        self.file = open(self.path, 'rb+')
        try:
            self.mmap = mmap.mmap(self.file.fileno(), 0)
        except OSError as e:
            self.file.close()
            self.closed = True
            raise Corruption(f'Empty chunk file!')
        try:
            self.block_size, self.min_ts = STRUCT_LQ.unpack(self.mmap[0:HEADER_SIZE+TIMESTAMP_SIZE])
            self.block_size_plus = self.block_size + TIMESTAMP_SIZE
        except struct.error:
            self.close()
            raise Corruption('Could not read the header of the chunk file %s' % (self.path, ))

        self.mmap.madvise(mmap.MADV_DONTNEED, 0, HEADER_SIZE+TIMESTAMP_SIZE)

        self.entries, = STRUCT_L.unpack(self.mmap[self.file_size-FOOTER_SIZE:self.file_size])
        self.pointer = self.entries*self.block_size_plus+HEADER_SIZE
        self.max_ts = self.get_timestamp_at(self.entries-1)

    cpdef unsigned int find_left(self, unsigned long long timestamp):
        """
        Return an index i of position such that ts[i] <= timestamp and
        (timestamp-ts[i]) -> min.
        
        Used as bound in searches: you start from this index and finish at 
        :meth:`~tempsdb.chunks.Chunk.find_right`.
        
        :param timestamp: timestamp to look for, must be smaller or equal to largest element
            in the chunk
        :type timestamp: int
        :return: index such that ts[i] <= timestamp and (timestamp-ts[i]) -> min, or length of the 
            array if timestamp is larger than largest element in this chunk
        :rtype: int
        """
        cdef:
            unsigned int hi = self.length()
            unsigned int lo = 0
            unsigned int mid
        while lo < hi:
            mid = (lo+hi)//2
            if self.get_timestamp_at(mid) < timestamp:
                lo = mid+1
            else:
                hi = mid
        return lo

    cpdef unsigned int find_right(self, unsigned long long timestamp):
        """
        Return an index i of position such that ts[i] > timestamp and
        (ts[i]-timestamp) -> min
        
        Used as bound in searches: you start from 
        :meth:`~tempsdb.chunks.Chunk.find_right` and finish at this inclusive. 
        
        :param timestamp: timestamp to look for
        :type timestamp: int
        :return: index such that ts[i] > timestamp and (ts[i]-timestamp) -> min
        :rtype: int 
        """
        cdef:
            unsigned int hi = self.length()
            unsigned int lo = 0
            unsigned int mid
        while lo < hi:
            mid = (lo+hi)//2
            if timestamp < self.get_timestamp_at(mid):
                hi = mid
            else:
                lo = mid+1
        return lo

    def __getitem__(self, index: tp.Union[int, slice]):
        if isinstance(index, slice):
            return self.iterate_range(index.start, index.stop)
        else:
            return self.get_piece_at(index)

    cdef int sync(self) except -1:
        """
        Synchronize the mmap
        """
        self.mmap.flush()
        return 0

    cdef int extend(self) except -1:
        """
        Adds PAGE_SIZE bytes to this file
        """
        self.file_size += self.page_size
        self.file.seek(0, 2)
        cdef bytearray ba = bytearray(self.page_size)
        ba[self.page_size-FOOTER_SIZE:self.page_size] = STRUCT_L.pack(self.entries)
        self.file.write(ba)
        self.mmap.resize(self.file_size)

    cdef unsigned long long get_timestamp_at(self, unsigned int index):
        """
        Get timestamp at given entry
        
        :param index: index of the entry
        :type index: int
        :return: timestamp at this entry
        :rtype: int
        """
        cdef unsigned long offset = HEADER_SIZE+index*self.block_size_plus
        return STRUCT_Q.unpack(self.mmap[offset:offset+TIMESTAMP_SIZE])[0]

    cpdef int delete(self) except -1:
        """
        Close and delete this chunk.
        """
        self.close()
        os.unlink(self.path)
        return 0

    cpdef int append(self, unsigned long long timestamp, bytes data) except -1:
        """
        Append a record to this chunk.
        
        Might range from very fast (just a memory operation) to quite slow (adding a new page
        to the file).
        
        Simultaneous writing is not thread-safe.
        
        :param timestamp: timestamp of the entry
        :type timestamp: int
        :param data: data to write
        :type data: bytes
        :raises InvalidState: chunk is closed
        :raises ValueError: invalid timestamp or data
        """
        if self.closed:
            raise InvalidState('chunk is closed')
        if len(data) != self.block_size:
            raise ValueError('data (%s) not equal in length to block size (%s)!' % (
                len(data), self.block_size
            ))
        if timestamp <= self.max_ts:
            raise ValueError('invalid timestamp')

        if self.pointer >= self.file_size-FOOTER_SIZE-self.block_size_plus:
            self.extend()

        # Append entry
        self.mmap[self.pointer:self.pointer+TIMESTAMP_SIZE] = STRUCT_Q.pack(timestamp)
        self.mmap[self.pointer+TIMESTAMP_SIZE:self.pointer+TIMESTAMP_SIZE+self.block_size] = data
        self.entries += 1
        # Update entries count
        self.mmap[self.file_size-FOOTER_SIZE:self.file_size] = STRUCT_L.pack(self.entries)
        # Update properties
        self.max_ts = timestamp
        self.pointer += self.block_size_plus
        return 0

    cpdef object iterate_indices(self, unsigned long starting_entry, unsigned long stopping_entry):
        """
        Return a partial iterator starting at starting_entry and ending at stopping_entry (exclusive).
        
        :param starting_entry: index of starting entry
        :type starting_entry: int
        :param stopping_entry: index of stopping entry
        :type stopping_entry:
        :return: an iterator
        :rtype: tp.Iterator[tp.Tuple[int, bytes]]
        """
        return self._iterate(starting_entry, stopping_entry)

    def _iterate(self, starting_entry: int, stopping_entry: int):
        cdef int i
        for i in range(starting_entry, stopping_entry):
            yield self.get_piece_at(i)

    def __iter__(self) -> tp.Iterator[tp.Tuple[int, bytes]]:
        cdef int i
        for i in range(self.entries):
            yield self.get_piece_at(i)

    def __len__(self):
        return self.length()

    cpdef int close(self) except -1:
        """
        Close the chunk and close the allocated resources
        """
        if self.closed:
            return 0
        if self.parent:
            with self.parent.open_lock:
                del self.parent.open_chunks[self.name()]
        self.parent = None
        self.mmap.close()
        self.file.close()
        return 0

    def __del__(self):
        self.close()

    cdef tuple get_piece_at(self, unsigned int index):
        """
        Return a piece of data at a particular index, numbered from 0
        
        :return: at piece of data at given index
        :rtype: tp.Tuple[int, bytes]
        """
        if index >= self.entries:
            raise IndexError('Index too large')
        cdef:
            unsigned long starting_index = HEADER_SIZE + index * self.block_size_plus
            unsigned long stopping_index = starting_index + self.block_size_plus
            unsigned long long ts = STRUCT_Q.unpack(
                self.mmap[starting_index:starting_index+TIMESTAMP_SIZE])[0]
        return ts, self.mmap[starting_index+TIMESTAMP_SIZE:stopping_index]


cpdef Chunk create_chunk(TimeSeries parent, str path, unsigned long long timestamp,
                         bytes data, int page_size):
    """
    Creates a new chunk on disk
    
    :param parent: parent time series
    :type parent: TimeSeries
    :param path: path to the new chunk file
    :type path: str
    :param timestamp: timestamp for first entry to contain
    :type timestamp: int
    :param data: data of the first entry
    :type data: bytes
    :param page_size: size of a single page for storage
    :type page_size: int
    :raises ValueError: entries in data were not of equal size, or data was empty or data
        was not sorted by timestamp or same timestamp appeared twice
    :raises AlreadyExists: chunk already exists 
    """
    if os.path.exists(path):
        raise AlreadyExists('chunk already exists!')
    if not data:
        raise ValueError('Data is empty')
    file = open(path, 'wb')
    cdef:
        bytes b
        unsigned long long ts
        unsigned long block_size = len(data)
        unsigned long file_size = 0
        unsigned long long last_ts = 0
        unsigned int entries = 1
        bint first_element = True
    file_size += file.write(STRUCT_L.pack(block_size))
    file_size += file.write(STRUCT_Q.pack(timestamp))
    file_size += file.write(data)

    # Pad this thing to page_size
    cdef unsigned long bytes_to_pad = page_size - (file_size % page_size)
    file.write(b'\x00' * bytes_to_pad)

    # Create a footer at the end
    cdef bytearray footer = bytearray(page_size)
    footer[-4:] = b'\x01\x00\x00\x00'   # 1 in little endian
    file.write(footer)
    file.close()
    return Chunk(parent, path, page_size)

