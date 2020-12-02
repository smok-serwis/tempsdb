from .chunks cimport Chunk
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

    cpdef void close(self)
    cdef int get_next(self) except -1
    cpdef tuple next_item(self)
