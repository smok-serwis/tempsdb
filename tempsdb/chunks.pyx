import os
import typing as tp
import struct
import mmap
from .exceptions import Corruption

STRUCT_QQL = struct.Struct('>QQL')
STRUCT_Q = struct.Struct('>Q')


cdef class Chunk:
    """
    Represents a single chunk of time series.

    This also implements an iterator interface. This will iterate with tp.Tuple[int, bytes].

    :param path: path to the chunk file
    :type path: str

    :ivar path: path to the chunk (str)
    :ivar min_ts: timestamp of the first entry stored (int)
    :ivar max_ts: timestamp of the last entry stored (int)
    :ivar block_size: size of the data entries (int)
    :ivar entries: amount of entries in this chunk (int)
    """
    def __init__(self, path: str):
        self.closed = False
        cdef unsigned long long file_size = os.path.getsize(path)
        self.path = path
        cdef bytes b
        print('Before open')
        self.file = open(self.path, 'rb+')
        try:
            self.mmap = mmap.mmap(self.file.fileno(), file_size, access=mmap.ACCESS_READ)
        except OSError as e:
            raise Corruption(f'Empty chunk file!')
        try:
            self.min_ts, self.max_ts, self.block_size = STRUCT_QQL.unpack(self.mmap[:20])
        except struct.error:
            raise Corruption('Could not read the header of the chunk file %s' % (self.path, ))
        self.pointer = 20
        self.entries = (file_size-self.pointer) // self.block_size

    def __iter__(self) -> tp.Iterator[tp.Tuple[int, bytes]]:
        cdef unsigned long i = 0
        for i in range(self.entries):
            yield self.get_piece_at(i)

    def __len__(self):
        return self.entries

    cpdef void close(self):
        """
        Close the chunk and close the allocated resources
        """
        if self.closed:
            return
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
            unsigned long starting_index = 20 + index * (self.block_size+8)
            unsigned long stopping_index = starting_index + self.block_size+8
            unsigned long long ts = STRUCT_Q.unpack(self.mmap[starting_index:starting_index+8])[0]
        return ts, self.mmap[starting_index+8:stopping_index]


cpdef Chunk create_chunk(str path, list data):
    """
    Creates a new chunk on disk
    
    :param path: path to the new chunk file
    :type path: str
    :param data: data to write, list of tuple (timestamp, entry to write).
        Must be nonempty and sorted by timestamp.
    :type data: tp.List[tp.Tuple[int, bytes]]
    :raises ValueError: entries in data were not of equal size, or data was empty or data
        was not sorted by timestamp or same timestamp appeared twice 
    """
    if not data:
        raise ValueError('Data is empty')
    file = open(path, 'wb')
    cdef:
        unsigned long long min_ts = 0xFFFFFFFFFFFFFFFF
        unsigned long long max_ts = 0
        bytes b
        unsigned long long ts
        unsigned long block_size = len(data[0][1])
        unsigned long long last_ts = 0
        bint first_element = True
    for ts, b in data:
        if ts < min_ts:
            min_ts = ts
        elif ts > max_ts:
            max_ts = ts

    file.write(STRUCT_QQL.pack(min_ts, max_ts, block_size))
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
    return Chunk(path)

