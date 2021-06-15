
cdef enum:
    MDV_JSON = 0
    MDV_MINIJSON = 1


cdef dict read_meta_at(str path)
cdef int write_meta_at(str path, dict meta)
