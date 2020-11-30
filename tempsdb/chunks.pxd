from .series cimport TimeSeries
import struct

STRUCT_Q = struct.Struct('<Q')
DEF HEADER_SIZE = 4
DEF TIMESTAMP_SIZE = 8

cdef class Chunk:
    cdef:
        TimeSeries parent
        readonly str path
        readonly unsigned long long min_ts
        readonly unsigned long long max_ts
        unsigned int block_size_plus
        readonly unsigned int block_size
        readonly unsigned long entries
        unsigned long file_size
        unsigned long pointer       # position to write next entry at
        unsigned long page_size
        object file
        object mmap
        bint closed
        readonly bint writable
        object write_lock

    cpdef object iterate_indices(self, unsigned long starting_entry, unsigned long stopping_entry)
    cpdef void close(self)
    cdef tuple get_piece_at(self, unsigned int index)
    cpdef int append(self, unsigned long long timestamp, bytes data) except -1
    cpdef int sync(self) except -1
    cpdef unsigned int find_left(self, unsigned long long timestamp)
    cpdef unsigned int find_right(self, unsigned long long timestamp)
    cdef int extend(self) except -1

    cdef inline unsigned long long name(self):
        """
        :return: the name of this chunk
        :rtype: int 
        """
        return self.min_ts

    cdef inline int length(self):
        """
        :return: amount of entries in this chunk
        :rtype: int 
        """
        return self.entries

    cdef unsigned long long get_timestamp_at(self, unsigned int index)


cpdef Chunk create_chunk(TimeSeries parent, str path, unsigned long long timestamp,
                         bytes data, int page_size)
