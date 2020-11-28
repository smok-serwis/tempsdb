cdef class Chunk:
    cdef:
        readonly str path
        readonly unsigned long long min_ts
        readonly unsigned long long max_ts
        readonly unsigned long block_size
        readonly unsigned long entries
        object file
        object mmap
        bint closed

    cpdef void close(self)
    cpdef tuple get_piece_at(self, unsigned int index)


cpdef Chunk create_chunk(str path, list data)
