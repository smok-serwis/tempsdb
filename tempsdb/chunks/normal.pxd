from .base cimport Chunk


cdef class NormalChunk(Chunk):
    cpdef int append(self, unsigned long long timestamp, bytes data) except -1
    cpdef int extend(self) except -1
