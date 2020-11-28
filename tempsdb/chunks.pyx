import os
import typing as tp
import struct
import mmap
from .exceptions import Corruption

STRUCT_QQL = struct.Struct('>QQL')
STRUCT_Q = struct.Struct('>Q')


cdef class Chunk:
    """
    Represents a single chunk of time series

    :param path: path to the chunk file
    :type path: str

    :ivar path: path to the chunk (str)
    :ivar min_ts: timestamp of the first entry stored (int)
    :ivar max_ts: timestamp of the last entry stored (int)
    :ivar block_size: size of the data entries (int)
    """
    def __init__(self, path: str):
        self.closed = False
        self.path = path
        cdef bytes b
        self.file = open(self.path, 'rb')
        try:
            self.mmap = mmap.mmap(self.file.fileno(), 0)
        except OSError:
            raise Corruption('Empty chunk file!')
        try:
            self.min_ts, self.max_ts, self.block_size = STRUCT_QQL.unpack(self.file.read(16))
        except IOError:
            raise Corruption('Could not read the header of the chunk file %s' % (self.path, ))
        self.pointer = 8
        self.entries = (os.path.getsize(self.path)-20) / self.block_size

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
            unsigned long starting_index = 20 + index * self.block_size
            unsigned long stopping_index = starting_index + self.block_size
            bytes bytes_at = self.mmap[starting_index:stopping_index]
            unsigned long long ts = STRUCT_Q.unpack(self.mmap[starting_index:starting_index+8])[0]
        return ts, self.mmap[starting_index+8:stopping_index]



cpdef Chunk create_chunk(str path, list data):
    """
    Creates a new chunk on disk
    
    :param path: path to the new chunk file
    :type path: str
    :param data: data to write, must be nonempty
    :type data: tp.List[tp.Tuple[int, bytes]]
    :raises ValueError: entries in data were not of equal size, or data was empty
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
    for ts, b in data:
        if ts < min_ts:
            min_ts = ts
        elif ts > max_ts:
            max_ts = ts

    file.write(STRUCT_QQL.pack(min_ts, max_ts, len(data[0][1])))
    try:
        for ts, b in data:
            if len(b) != block_size:
                raise ValueError('Block size has entries of not equal length')
            file.write(STRUCT_Q.pack(ts))
            file.write(b)
        file.close()
    except ValueError:
        file.close()
        os.unlink(path)
        raise
    return Chunk(path)

