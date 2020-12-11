from .series cimport TimeSeries


cdef class AlternativeMMap:
    cdef:
        object io, file_lock_object
        unsigned long size


cdef class Chunk:
    cdef:
        TimeSeries parent
        readonly str path
        readonly unsigned long long min_ts
        readonly unsigned long long max_ts
        unsigned int block_size_plus        # block size plus timestamp length
        readonly unsigned int block_size
        readonly unsigned long entries
        unsigned long file_size
        unsigned long pointer       # position to write next entry at
        readonly unsigned long page_size
        object file, mmap, file_lock_object
        bint closed

    cdef void incref(self)
    cdef int decref(self) except -1
    cpdef object iterate_indices(self, unsigned long starting_entry, unsigned long stopping_entry)
    cpdef int close(self, bint force=*) except -1
    cpdef unsigned long long get_timestamp_at(self, unsigned int index)
    cdef tuple get_piece_at(self, unsigned int index)
    cpdef bytes get_value_at(self, unsigned int index)
    cpdef bytes get_slice_of_piece_at(self, unsigned int index, int start, int stop)
    cpdef bytes get_slice_of_piece_starting_at(self, unsigned int index, int start)
    cpdef int get_byte_of_piece(self, unsigned int index, int byte_index) except -1
    cpdef int append(self, unsigned long long timestamp, bytes data) except -1
    cdef int sync(self) except -1
    cpdef unsigned int find_left(self, unsigned long long timestamp)
    cpdef unsigned int find_right(self, unsigned long long timestamp)
    cdef int extend(self) except -1
    cpdef int delete(self) except -1
    cpdef int switch_to_descriptor_based_access(self) except -1
    cpdef int switch_to_mmap_based_access(self) except -1
    cpdef unsigned long get_mmap_size(self)

    cdef inline unsigned long long name(self):
        """
        :return: the name of this chunk
        :rtype: int 
        """
        return self.min_ts

    cdef inline int length(self):
        """
        :return: amount of entries in this chunk
        :rtype: int 
        """
        return self.entries


cpdef Chunk create_chunk(TimeSeries parent, str path, unsigned long long timestamp,
                         bytes data, int page_size,
                         bint descriptor_based_access=*)
