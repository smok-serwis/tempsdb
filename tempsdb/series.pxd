from .database cimport Database

cdef class TimeSeries:
    cdef:
        str path
        Database parent
        str name
        int block_size
        unsigned long long last_entry_ts
        list chunks

    cpdef void sync(self)
