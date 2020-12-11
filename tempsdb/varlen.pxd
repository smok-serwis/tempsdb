from .series cimport TimeSeries


cdef class VarlenSeries:
    cdef:
        bint closed
        int size_field
        object size_struct
        readonly str path
        readonly str name
        TimeSeries root_series
        list series
        list length_profile
        int max_entries_per_chunk
        int current_maximum_length

    cpdef int close(self) except -1
    cpdef int get_length_for(self, int index)
    cpdef int add_series(self) except -1
    cpdef int append(self, unsigned long long timestamp, bytes data) except -1

cpdef VarlenSeries create_varlen_series(str path, str name, int size_struct, list length_profile,
                                        int max_entries_per_chunk)
