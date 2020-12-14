from .chunks.base cimport Chunk
from .series cimport TimeSeries


cdef class Iterator:
    cdef:
        unsigned long long start
        unsigned long long stop
        object chunks       # type: collections.deque
        bint is_first, is_last
        TimeSeries parent
        unsigned int i, limit
        bint closed
        Chunk current_chunk

    cpdef int close(self) except -1
    cdef int get_next(self) except -1
    cpdef tuple next_item(self)
    cdef tuple next_item_pos(self)
