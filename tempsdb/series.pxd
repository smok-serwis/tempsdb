from .chunks.base cimport Chunk
from .iterators cimport Iterator


cdef class TimeSeries:
    cdef:
        bint closed
        object lock             # lock to hold while writing
        object open_lock        # lock to hold while opening or closing chunks
        readonly str path
        readonly str name
        unsigned int max_entries_per_chunk
        readonly unsigned long long last_entry_synced
        readonly int block_size
        readonly unsigned long long last_entry_ts
        readonly int gzip_level
        unsigned int page_size
        readonly dict metadata
        readonly bint descriptor_based_access
        list chunks
        dict refs_chunks        # type: tp.Dict[int, int]
        dict open_chunks        # type: tp.Dict[int, Chunk]
        Chunk last_chunk
        object mpm      # satella.instrumentation.memory.MemoryPressureManager


    cdef void register_memory_pressure_manager(self, object mpm)
    cpdef int delete(self) except -1
    cdef dict get_metadata(self)
    cpdef int close(self) except -1
    cdef void incref_chunk(self, unsigned long long name)
    cdef void decref_chunk(self, unsigned long long name)
    cdef Chunk open_chunk(self, unsigned long long name, bint is_direct, bint is_gzip)
    cdef int sync_metadata(self) except -1
    cpdef int mark_synced_up_to(self, unsigned long long timestamp) except -1
    cpdef int append(self, unsigned long long timestamp, bytes data) except -1
    cpdef int append_padded(self, unsigned long long timestamp, bytes data) except -1
    cpdef int sync(self) except -1
    cpdef int close_chunks(self) except -1
    cpdef Iterator iterate_range(self, unsigned long long start, unsigned long long stop,
                                 bint direct_bytes=*)
    cdef unsigned int get_index_of_chunk_for(self, unsigned long long timestamp)
    cpdef int trim(self, unsigned long long timestamp) except -1
    cpdef unsigned long open_chunks_mmap_size(self)
    cpdef tuple get_current_value(self)
    cpdef int disable_mmap(self) except -1
    cpdef int enable_mmap(self) except -1
    cpdef int set_metadata(self, dict new_meta) except -1
    cdef inline int get_references_for(self, unsigned long long timestamp):
        return self.refs_chunks.get(timestamp, 0)

cpdef TimeSeries create_series(str path, str name, unsigned int block_size,
                               int max_entries_per_chunk, int page_size=*,
                               bint use_descriptor_based_access=*,
                               int gzip_level=*)
