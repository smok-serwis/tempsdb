from .base cimport Chunk
from ..series cimport TimeSeries


cpdef Chunk create_chunk(TimeSeries parent, str path, unsigned long long timestamp,
                         bytes data, int page_size, bint descriptor_based_access=*,
                         bint use_direct_mode=*, int gzip_compresion_level=*)
