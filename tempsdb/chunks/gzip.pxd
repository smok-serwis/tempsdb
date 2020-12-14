cdef class ReadWriteGzipFile:
    cdef:
        str path
        object ro_file, rw_file
        int compress_level
        object lock
        unsigned long pointer
        unsigned long size
        bint needs_flush_before_read

    cpdef int flush(self) except -1

