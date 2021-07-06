from .series cimport TimeSeries
from .varlen cimport VarlenSeries

cdef class Database:
    cdef:
        readonly str path
        bint closed
        object lock
        object mpm, mpm_handler
        dict open_series
        dict open_varlen_series
        readonly dict metadata

    cpdef int checkpoint(self) except -1
    cpdef int reload_metadata(self) except -1
    cpdef int set_metadata(self, dict meta) except -1
    cpdef int close(self) except -1
    cpdef TimeSeries get_series(self, str name,
                                bint use_descriptor_based_access=*)
    cpdef VarlenSeries get_varlen_series(self, str name)
    cpdef int register_memory_pressure_manager( self, object mpm) except -1
    cpdef TimeSeries create_series(self, str name, int block_size,
                                   unsigned long entries_per_chunk,
                                   int page_size=*,
                                   bint use_descriptor_based_access=*,
                                   int gzip_level=*)
    cpdef VarlenSeries create_varlen_series(self, str name, list length_profile,
                                            int size_struct,
                                            unsigned long entries_per_chunk,
                                            int gzip_level=*)
    cpdef int delete_series(self, str name) except -1
    cpdef int delete_varlen_series(self, str name) except -1
    cpdef list get_open_series(self)
    cpdef list get_all_normal_series(self)
    cpdef list get_all_varlen_series(self)
    cpdef int close_all_open_series(self) except -1
    cpdef unsigned long long get_first_entry_for(self, str name)
    cpdef int sync(self) except -1

cpdef Database create_database(str path)
cpdef int disable_logging() except -1
