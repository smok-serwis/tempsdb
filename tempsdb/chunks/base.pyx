import io
import os
import threading
import typing as tp
import struct
import mmap

from .gzip cimport ReadWriteGzipFile
from ..exceptions import Corruption, InvalidState, AlreadyExists, StillOpen
from ..series cimport TimeSeries


STRUCT_Q = struct.Struct('<Q')
STRUCT_L = struct.Struct('<L')
STRUCT_LQ = struct.Struct('<LQ')

DEF HEADER_SIZE = 4
DEF TIMESTAMP_SIZE = 8
DEF FOOTER_SIZE = 4


cdef class AlternativeMMap:
    """
    An alternative mmap implementation used when mmap cannot allocate due to memory issues.

    Note that opening gzip files is slow, as the script needs to iterate
    """
    def flush(self):
        self.io.flush()

    def madvise(self, a, b, c):
        ...

    def resize(self, int file_size):
        self.size = file_size

    def __init__(self, io_file: io.BinaryIO, file_lock_object):
        self.io = io_file
        if isinstance(io_file, ReadWriteGzipFile):
            self.size = io_file.size()
        else:
            self.io.seek(0, 2)
            self.size = self.io.tell()
        self.file_lock_object = file_lock_object

    def __getitem__(self, item: slice) -> bytes:
        cdef:
            unsigned long start = item.start
            unsigned long stop = item.stop
            bytes b

        with self.file_lock_object:
            if start != self.io.tell():
                self.io.seek(start, 0)
            return self.io.read(stop-start)

    def __setitem__(self, key: slice, value: bytes):
        cdef:
            unsigned long start = key.start

        with self.file_lock_object:
            if not isinstance(self.io, ReadWriteGzipFile):
                self.io.seek(0, 2)
            self.io.write(value)

    def close(self):
        pass


cdef class Chunk:
    """
    Represents a single chunk of time series.

    This implementation is the default - it allocates a page ahead of the stream and
    writes the amount of currently written entries to the end of the page. Suitable for SSD
    and RAM media.

    This also implements an iterator interface, and will iterate with tp.Tuple[int, bytes],
    as well as a sequence protocol.

    This will try to mmap opened files, but if mmap fails due to not enough memory this
    will use descriptor-based access.

    :param parent: parent time series
    :param path: path to the chunk file
    :param use_descriptor_access: whether to use descriptor based access instead of mmap

    :ivar path: path to the chunk (str)
    :ivar min_ts: timestamp of the first entry stored (int)
    :ivar max_ts: timestamp of the last entry stored (int)
    :ivar block_size: size of the data entries (int)
    :ivar entries: amount of entries in this chunk (int)
    :ivar page_size: size of the page (int)
    """
    cpdef unsigned long get_mmap_size(self):
        """
        :return: how many bytes are mmaped?
        :rtype: int
        """
        if isinstance(self.mmap, AlternativeMMap):
            return 0
        else:
            return self.file_size

    cpdef int switch_to_descriptor_based_access(self) except -1:
        """
        Switch self to descriptor-based access instead of mmap.
        
        No-op if already in descriptor based mode.
        """
        if isinstance(self.mmap, AlternativeMMap):
            return 0
        self.mmap.close()
        self.file_lock_object = threading.Lock()
        self.mmap = AlternativeMMap(self.file, self.file_lock_object)
        return 0

    cpdef int after_init(self) except -1:
        """
        Load the :attr:`~Chunk.entries`, :attr:`~Chunk.pointer` and :attr:`~Chunk.max_ts`
        
        :meta private:
        """
        self.entries, = STRUCT_L.unpack(self.mmap[self.file_size-FOOTER_SIZE:self.file_size])
        self.pointer = self.entries*self.block_size_plus+HEADER_SIZE
        self.max_ts = self.get_timestamp_at(self.entries-1)

        if self.pointer >= self.page_size:
            # Inform the OS that we don't need the header anymore
            self.mmap.madvise(mmap.MADV_DONTNEED, 0, HEADER_SIZE+TIMESTAMP_SIZE)
        return 0

    cpdef object open_file(self, str path):
        return open(self.path, 'rb+')

    def __init__(self, parent: tp.Optional[TimeSeries], path: str, page_size: int,
                 use_descriptor_access: bool = False):
        cdef bytes b
        self.file_size = os.path.getsize(path)
        self.page_size = page_size
        self.parent = parent
        self.closed = False
        self.path = path
        self.file = self.open_file(path)
        self.file_lock_object = None

        if use_descriptor_access:
            self.file_lock_object = threading.Lock()
            self.mmap = AlternativeMMap(self.file, self.file_lock_object)
        else:
            try:
                self.mmap = mmap.mmap(self.file.fileno(), 0)
            except OSError as e:
                if e.errno in (11, 12):   # Cannot allocate memory or memory range exhausted
                    self.file_lock_object = threading.Lock()
                    self.mmap = AlternativeMMap(self.file, self.file_lock_object)
                else:
                    self.file.close()
                    self.closed = True
                    raise Corruption(f'Failed to mmap chunk file: {e}')

        try:
            self.block_size, self.min_ts = STRUCT_LQ.unpack(self.mmap[0:HEADER_SIZE+TIMESTAMP_SIZE])
            self.block_size_plus = self.block_size + TIMESTAMP_SIZE
        except struct.error:
            self.close()
            raise Corruption('Could not read the header of the chunk file %s' % (self.path, ))

        self.entries = 0
        self.max_ts = 0
        self.pointer = 0

        self.after_init()

    cpdef int get_byte_of_piece(self, unsigned int index, int byte_index) except -1:
        """
        Return a particular byte of given element at given index.
        
        When index is negative, or larger than block_size, the behaviour is undefined
        
        :param index: index of the element
        :param byte_index: index of the byte
        :return: value of the byte
        :raises ValueError: index too large
        """
        if index > self.entries:
            raise ValueError('index too large')
        cdef:
            unsigned long offset = HEADER_SIZE + TIMESTAMP_SIZE + index * self.block_size_plus + byte_index
        return self.mmap[offset]

    cpdef bytes get_slice_of_piece_starting_at(self, unsigned int index, int start):
        """
        Return a slice of data from given element starting at given index to the end
        
        :param index: index of the element
        :param start: starting index
        :return: a byte slice
        """
        return self.get_slice_of_piece_at(index, start, self.block_size)

    cpdef bytes get_slice_of_piece_at(self, unsigned int index, int start, int stop):
        """
        Return a slice of data from given element
        
        :param index: index of the element
        :param start: starting offset of data
        :param stop: stopping offset of data
        :return: a byte slice
        """
        if index >= self.entries:
            raise IndexError('Index too large')
        cdef:
            unsigned long starting_index = HEADER_SIZE + TIMESTAMP_SIZE + index * self.block_size_plus + start
            unsigned long stopping_index = starting_index + stop
        return self.mmap[starting_index:stopping_index]

    cpdef unsigned long long get_timestamp_at(self, unsigned int index):
        """
        Return a timestamp at a particular location
        
        Passing an invalid index will result in an undefined behaviour.
        
        :param index: index of element
        :return: the timestamp
        """
        cdef:
            unsigned long starting_index = HEADER_SIZE + index * self.block_size_plus
            unsigned long stopping_index = starting_index + TIMESTAMP_SIZE
        return STRUCT_Q.unpack(self.mmap[starting_index:stopping_index])[0]

    cpdef unsigned int find_left(self, unsigned long long timestamp):
        """
        Return an index i of position such that ts[i] <= timestamp and
        (timestamp-ts[i]) -> min.
        
        Used as bound in searches: you start from this index and finish at 
        :meth:`~tempsdb.chunks.Chunk.find_right`.
        
        :param timestamp: timestamp to look for, must be smaller or equal to largest element
            in the chunk
        :return: index such that ts[i] <= timestamp and (timestamp-ts[i]) -> min, or length of the 
            array if timestamp is larger than largest element in this chunk
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
        :return: index such that ts[i] > timestamp and (ts[i]-timestamp) -> min
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

    cpdef int extend(self) except -1:
        """
        Adds PAGE_SIZE bytes to this file
        """
        cdef bytearray ba
        if self.file_lock_object:
            self.file_lock_object.acquire()
        try:
            self.mmap.seek(self.file_size)
            self.file_size += self.page_size
            ba = bytearray(self.page_size)
            ba[self.page_size-FOOTER_SIZE:self.page_size] = STRUCT_L.pack(self.entries)
            self.file.write(ba)
            self.mmap.resize(self.file_size)
        finally:
            if self.file_lock_object:
                self.file_lock_object.release()

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
        
        Timestamp and data is not checked for, this is supposed to be handled by
        :class:`~tempsdb.series.TimeSeries`.
        
        :param timestamp: timestamp of the entry
        :param data: data to write
        :raises InvalidState: chunk is closed
        """
        if self.closed:
            raise InvalidState('chunk is closed')

        if self.pointer >= self.file_size-FOOTER_SIZE-self.block_size_plus:
            self.extend()
        cdef unsigned long long ptr_end = self.pointer + TIMESTAMP_SIZE
        # Append entry
        cdef bytes b = STRUCT_Q.pack(timestamp) + data
        self.mmap[self.pointer:ptr_end+self.block_size] = b
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
        :param stopping_entry: index of stopping entry
        :return: an iterator
        :rtype: tp.Iterator[tp.Tuple[int, bytes]]
        """
        return self._iterate(starting_entry, stopping_entry)

    def _iterate(self, starting_entry: int, stopping_entry: int):
        cdef int i
        for i in range(starting_entry, stopping_entry):
            yield self.get_piece_at(i)

    def __iter__(self) -> tp.Iterator[tp.Tuple[int, bytes]]:
        return self._iterate(0, self.entries)

    def __len__(self):
        return self.length()

    cdef void incref(self):
        if self.parent is not None:
            self.parent.incref_chunk(self.min_ts)

    cdef int decref(self) except -1:
        if self.parent is not None:
            self.parent.decref_chunk(self.name())
            if self.parent.get_references_for(self.name()) < 0:
                raise ValueError('reference of chunk fell below zero!')
        return 0

    cpdef bytes get_value_at(self, unsigned int index):
        """
        Return only the value at a particular index, numbered from 0
        
        :return: value at given index
        """
        if index >= self.entries:
            raise IndexError('Index too large')
        cdef:
            unsigned long starting_index = HEADER_SIZE + TIMESTAMP_SIZE + index * self.block_size_plus
            unsigned long stopping_index = starting_index + self.block_size
        return self.mmap[starting_index:stopping_index]

    cpdef int close(self, bint force=False) except -1:
        """
        Close the chunk and close the allocated resources
        """
        if self.closed:
            return 0
        cdef unsigned long long name = self.name()
        if self.parent:
            with self.parent.open_lock:
                if self.parent.get_references_for(name) and not force:
                    raise StillOpen('chunk still open!')
                del self.parent.open_chunks[name]
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

