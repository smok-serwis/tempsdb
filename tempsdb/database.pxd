from .series cimport TimeSeries


cdef class Database:
    cdef:
        str path
        bint closed
        object lock

    cpdef void close(self)
    cpdef TimeSeries get_series(self, str name)

cpdef Database create_database(str path)

