import os
import threading
import typing as tp
import struct
import mmap
from .exceptions import Corruption, InvalidState, AlreadyExists
from .series cimport TimeSeries

STRUCT_L = struct.Struct('>L')
STRUCT_Q = struct.Struct('>Q')
DEF HEADER_SIZE = 4
DEF TIMESTAMP_SIZE = 8


cdef class Chunk:
    """
    Represents a single chunk of time series.

    This also implements an iterator interface, and will iterate with tp.Tuple[int, bytes].

    :param path: path to the chunk file
    :type path: str

    :ivar path: path to the chunk (str)
    :ivar min_ts: timestamp of the first entry stored (int)
    :ivar max_ts: timestamp of the last entry stored (int)
    :ivar block_size: size of the data entries (int)
    :ivar entries: amount of entries in this chunk (int)
    :ivar writable: is this chunk writable (bool)
    """
    def __init__(self, parent: tp.Optional[TimeSeries], path: str, writable: bool = True):
        cdef:
            unsigned long long file_size = os.path.getsize(path)
            bytes b
        self.parent = parent
        self.writable = writable
        self.write_lock = threading.Lock()
        self.closed = False
        self.path = path
        self.file = open(self.path, 'rb+' if self.writable else 'rb')
        try:
            if self.writable:
                self.mmap = mmap.mmap(self.file.fileno(), file_size)
            else:
                self.mmap = mmap.mmap(self.file.fileno(), file_size, access=mmap.ACCESS_READ)
        except OSError as e:
            self.file.close()
            self.closed = True
            raise Corruption(f'Empty chunk file!')
        try:
            self.block_size, = STRUCT_L.unpack(self.mmap[:HEADER_SIZE])
        except struct.error:
            self.close()
            raise Corruption('Could not read the header of the chunk file %s' % (self.path, ))
        self.entries = (file_size-HEADER_SIZE) // (self.block_size+TIMESTAMP_SIZE)
        self.max_ts, = STRUCT_Q.unpack(self.mmap[-TIMESTAMP_SIZE-self.block_size:-self.block_size])
        self.min_ts, = STRUCT_Q.unpack(self.mmap[HEADER_SIZE:HEADER_SIZE+TIMESTAMP_SIZE])

    cpdef int put(self, unsigned long long timestamp, bytes data) except -1:
        """
        Append a record to this chunk
        
        :param timestamp: timestamp of the entry
        :type timestamp: int
        :param data: data to write
        :type data: bytes
        :raises InvalidState: chunk is closed or not writable
        :raises ValueError: invalid timestamp or data
        """
        if self.closed or not self.writable:
            raise InvalidState('chunk is closed')
        if len(data) != self.block_size:
            raise ValueError('data not equal in length to block size!')
        if timestamp <= self.max_ts:
            raise ValueError('invalid timestamp')

        cdef bytearray data_to_write = bytearray(TIMESTAMP_SIZE+self.block_size)
        data_to_write[0:TIMESTAMP_SIZE] = STRUCT_Q.pack(timestamp)
        data_to_write[TIMESTAMP_SIZE:] = data
        with self.write_lock:
            self.file.seek(0, 2)
            self.file.write(data_to_write)
            self.mmap.resize((self.entries+1)*(8+self.block_size)+HEADER_SIZE)
            self.entries += 1
            self.max_ts = timestamp
        return 0

    def __iter__(self) -> tp.Iterator[tp.Tuple[int, bytes]]:
        cdef unsigned long i = 0
        for i in range(self.entries):
            yield self.get_piece_at(i)

    def __len__(self):
        return self.length()

    cpdef void close(self):
        """
        Close the chunk and close the allocated resources
        """
        if self.closed:
            return
        if self.parent:
            with self.parent.fopen_lock:
                del self.parent.open_chunks[self.min_ts]
        self.parent = None
        self.mmap.close()
        self.file.close()

    def __del__(self):
        self.close()

    cpdef tuple get_piece_at(self, unsigned int index):
        """
        Return a piece of data at a particular index, numbered from 0
        
        :return: at piece of data at given index
        :rtype: tp.Tuple[int, bytes]
        """
        if index >= self.entries:
            raise IndexError('Index too large')
        cdef:
            unsigned long starting_index = HEADER_SIZE + index * (self.block_size+TIMESTAMP_SIZE)
            unsigned long stopping_index = starting_index + self.block_size+TIMESTAMP_SIZE
            unsigned long long ts = STRUCT_Q.unpack(
                self.mmap[starting_index:starting_index+TIMESTAMP_SIZE])[0]
        return ts, self.mmap[starting_index+TIMESTAMP_SIZE:stopping_index]


cpdef Chunk create_chunk(TimeSeries parent, str path, list data):
    """
    Creates a new chunk on disk
    
    :param parent: parent time series
    :type parent: TimeSeries
    :param path: path to the new chunk file
    :type path: str
    :param data: data to write, list of tuple (timestamp, entry to write).
        Must be nonempty and sorted by timestamp.
    :type data: tp.List[tp.Tuple[int, bytes]]
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
        unsigned long block_size = len(data[0][1])
        unsigned long long last_ts = 0
        bint first_element = True

    file.write(STRUCT_L.pack(block_size))
    try:
        for ts, b in data:
            if not first_element:
                if ts <= last_ts:
                    raise ValueError('Timestamp appeared twice or data was not sorted')
            if len(b) != block_size:
                raise ValueError('Block size has entries of not equal length')
            file.write(STRUCT_Q.pack(ts))
            file.write(b)
            last_ts = ts
            first_element = False
    except ValueError:
        file.close()
        os.unlink(path)
        raise
    file.close()
    return Chunk(parent, path)

