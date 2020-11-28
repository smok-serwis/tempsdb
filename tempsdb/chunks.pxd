from .series cimport TimeSeries

cdef class Chunk:
    cdef:
        TimeSeries parent
        readonly str path
        readonly unsigned long long min_ts
        readonly unsigned long long max_ts
        unsigned int block_size_plus
        readonly unsigned int block_size
        readonly unsigned long entries
        object file
        object mmap
        bint closed
        readonly bint writable
        object write_lock

    cpdef object iterate_range(self, unsigned long starting_entry, unsigned long stopping_entry)
    cpdef void close(self)
    cpdef tuple get_piece_at(self, unsigned int index)
    cpdef int append(self, unsigned long long timestamp, bytes data) except -1
    cpdef int sync(self) except -1
    cdef inline int length(self):
        return self.entries

cpdef Chunk create_chunk(TimeSeries parent, str path, list data)
