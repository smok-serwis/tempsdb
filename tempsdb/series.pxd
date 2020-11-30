from .chunks cimport Chunk


cdef class TimeSeries:
    cdef:
        bint closed
        object lock             # lock to hold while writing
        object open_lock        # lock to hold while opening or closing chunks
        readonly str path
        unsigned int max_entries_per_chunk
        readonly unsigned long long last_entry_synced
        readonly unsigned int block_size
        readonly unsigned long long last_entry_ts
        unsigned int page_size
        list chunks
        dict open_chunks
        list data_in_memory
        Chunk last_chunk
        object mpm      # satella.instrumentation.memory.MemoryPressureManager

    cpdef void register_memory_pressure_manager(self, object mpm)
    cpdef int delete(self) except -1
    cdef dict _get_metadata(self)
    cpdef void close(self)
    cpdef Chunk open_chunk(self, unsigned long long name)
    cpdef int mark_synced_up_to(self, unsigned long long timestamp) except -1
    cpdef int append(self, unsigned long long timestamp, bytes data) except -1
    cpdef int sync(self) except -1
    cpdef int close_chunks(self) except -1

cpdef TimeSeries create_series(str path, unsigned int block_size,
                               int max_entries_per_chunk, int page_size=*)
