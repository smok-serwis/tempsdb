from .database cimport Database

cdef class TimeSeries:
    cdef:
        object lock
        str path
        Database parent
        str name
        unsigned int max_entries_per_chunk
        double last_synced
        readonly double interval_between_synces
        readonly unsigned long long last_entry_synced
        readonly unsigned int block_size
        readonly unsigned long long last_entry_ts
        list chunks
        dict open_chunks
        list data_in_memory

    cdef dict _get_metadata(self)
    cpdef int mark_synced_up_to(self, unsigned long long timestamp) except -1
    cpdef int try_sync(self) except -1
    cpdef int _sync_metadata(self) except -1
    cpdef int put(self, unsigned long long timestamp, bytes data) except -1
    cpdef int sync(self) except -1

