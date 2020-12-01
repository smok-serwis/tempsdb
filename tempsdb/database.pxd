from .series cimport TimeSeries


cdef class Database:
    cdef:
        readonly str path
        bint closed
        object lock
        object mpm
        dict open_series

    cpdef int close(self) except -1
    cpdef TimeSeries get_series(self, str name,
                                bint use_descriptor_based_access=*)
    cpdef int register_memory_pressure_manager(self, object mpm) except -1
    cpdef TimeSeries create_series(self, str name, int block_size,
                                   unsigned long entries_per_chunk,
                                   int page_size=*,
                                   bint use_descriptor_based_access=*)
    cpdef list get_open_series(self)
    cpdef list get_all_series(self)
    cpdef int close_all_open_series(self) except -1
    cpdef unsigned long long get_first_entry_for(self, str name)
    cpdef int sync(self) except -1

cpdef Database create_database(str path)

