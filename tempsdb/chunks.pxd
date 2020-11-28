cdef class Chunk:
    cdef:
        readonly str path
        readonly unsigned long long min_ts
        readonly unsigned long long max_ts
        readonly unsigned long block_size
        readonly unsigned long entries
        object file
        object mmap
        bint closed, writable
        object write_lock

    cpdef void close(self)
    cpdef tuple get_piece_at(self, unsigned int index)
    cpdef int put(self, unsigned long long timestamp, bytes data) except -1
    cdef inline int length(self):
        return self.entries

cpdef Chunk create_chunk(str path, list data)
