from .series cimport TimeSeries


cdef class VarlenSeries:
    cdef:
        bint closed
        int size_field
        int references
        object size_struct
        readonly str path
        readonly str name
        TimeSeries root_series
        readonly list series
        readonly list length_profile
        readonly unsigned int max_entries_per_chunk
        int current_maximum_length
        object mpm
        int gzip_level
        bint mmap_enabled

    cpdef int enable_mmap(self) except -1
    cpdef int disable_mmap(self) except -1
    cpdef unsigned long open_chunks_mmap_size(self)
    cpdef int mark_synced_up_to(self, unsigned long long timestamp) except -1
    cpdef int close(self, bint force=*) except -1
    cpdef int delete(self) except -1
    cpdef tuple get_current_value(self)
    cdef int get_length_for(self, int index)
    cpdef int trim(self, unsigned long long timestamp) except -1
    cdef int add_series(self) except -1
    cpdef int close_chunks(self) except -1
    cdef void register_memory_pressure_manager(self, object mpm)
    cpdef int append(self, unsigned long long timestamp, bytes data) except -1
    cpdef long long get_maximum_length(self) except -1
    cpdef VarlenIterator iterate_range(self, unsigned long long start, unsigned long long stop,
                                       bint direct_bytes=*)


cdef class VarlenIterator:
    cdef:
        bint closed
        VarlenSeries parent
        list positions
        list timestamps
        list chunks
        list iterators
        unsigned long long start
        unsigned long long stop
        bint direct_bytes

    cpdef int close(self) except -1
    cpdef VarlenEntry get_next(self)
    cdef int advance_series(self, int index, bint force) except -1


cdef class VarlenEntry:
    cdef:
        list chunks
        list item_no
        VarlenSeries parent
        bytes data
        long len

    cpdef int length(self)
    cpdef bytes to_bytes(self)
    cpdef unsigned long long timestamp(self)
    cpdef bytes slice(self, int start, int stop)
    cpdef int get_byte_at(self, int index) except -1
    cpdef bint endswith(self, bytes v)
    cpdef bint startswith(self, bytes v)
    cpdef int close(self) except -1

cpdef VarlenSeries create_varlen_series(str path, str name, int size_struct, list length_profile,
                                        int max_entries_per_chunk,
                                        bint use_descriptor_based_access=*,
                                        int gzip_level=*)
