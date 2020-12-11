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
        object mpm

    cpdef int close(self) except -1
    cpdef int delete(self) except -1
    cpdef int get_length_for(self, int index)
    cpdef int add_series(self) except -1
    cdef void register_memory_pressure_manager(self, object mpm)
    cpdef int append(self, unsigned long long timestamp, bytes data) except -1
    cdef inline int get_maximum_length(self):
        if self.size_field == 1:
            return 0xFF
        elif self.size_field == 2:
            return 0xFFFF
        elif self.size_field == 4:
            return 0xFFFFFFFF

cdef class VarlenEntry:
    cdef:
        list chunks
        list item_no
        VarlenSeries parent

    cpdef int length(self)
    cpdef bytes to_bytes(self)
    cpdef unsigned long long timestamp(self)
    cpdef bytes slice(self, int start, int stop)
    cpdef int get_byte_at(self, int index) except -1


cpdef VarlenSeries create_varlen_series(str path, str name, int size_struct, list length_profile,
                                        int max_entries_per_chunk)
