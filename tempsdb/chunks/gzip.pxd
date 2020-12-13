cdef class ReadWriteGzipFile:
    cdef:
        str path
        object ro_file, rw_file
        int compress_level
        object lock
        unsigned long pointer

    cdef int reopen_read(self) except -1

