from .database cimport Database

cdef class TimeSeries:
    cdef:
        Database parent
        str name
