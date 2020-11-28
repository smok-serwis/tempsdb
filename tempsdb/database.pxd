cdef class Database:
    cdef:
        str path
        bint closed

    cpdef void close(self)
