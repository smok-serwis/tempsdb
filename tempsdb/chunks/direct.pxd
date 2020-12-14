from .base cimport Chunk


cdef class DirectChunk(Chunk):
    cdef:
        int gzip

    cpdef object open_file(self, str path)
    cpdef int after_init(self) except -1
    cpdef int append(self, unsigned long long timestamp, bytes data) except -1
