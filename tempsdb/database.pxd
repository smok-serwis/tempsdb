from .series cimport TimeSeries


cdef class Database:
    cdef:
        str path
        bint closed
        object lock
        object mpm

    cpdef void close(self)
    cpdef TimeSeries get_series(self, str name)
    cpdef void register_memory_pressure_manager(self, object mpm)
    cpdef TimeSeries create_series(self, str name, int block_size,
                                   unsigned long entries_per_chunk,
                                   int page_size=*)

cpdef Database create_database(str path)

