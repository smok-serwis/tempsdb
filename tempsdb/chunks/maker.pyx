import gzip
import os
import struct

from tempsdb.exceptions import AlreadyExists
from .base cimport Chunk
from .direct cimport DirectChunk
from ..series cimport TimeSeries

STRUCT_Q = struct.Struct('<Q')
STRUCT_L = struct.Struct('<L')

cpdef Chunk create_chunk(TimeSeries parent, str path, unsigned long long timestamp,
                         bytes data, int page_size, bint descriptor_based_access=False,
                         bint use_direct_mode = False, int gzip_compression_level=0):
    """
    Creates a new chunk on disk
    
    :param parent: parent time series
    :param path: path to the new chunk file
    :param timestamp: timestamp for first entry to contain
    :param data: data of the first entry
    :param page_size: size of a single page for storage 
    :param descriptor_based_access: whether to use descriptor based access instead of mmap. 
        Default is False
    :param use_direct_mode: if True, chunk will be created using direct mode, without page
        preallocation
    :param gzip_compression_level: gzip compression level. Use 0 to disable compression.
    :raises ValueError: entries in data were not of equal size, or data was empty or data
        was not sorted by timestamp or same timestamp appeared twice
    :raises AlreadyExists: chunk already exists 
    """
    cdef str original_path = path
    if os.path.exists(path):
        raise AlreadyExists('chunk already exists!')
    if not data:
        raise ValueError('Data is empty')
    if not gzip_compression_level and use_direct_mode:
        path = path + '.direct'
    elif gzip_compression_level:
        path = path + '.gz'

    if gzip_compression_level:
        file = gzip.open(path, 'wb', compresslevel=gzip_compression_level)
    else:
        file = open(path, 'wb')
    cdef:
        bytes b
        unsigned long long ts
        unsigned long block_size = len(data)
        unsigned long file_size = 0
        unsigned long long last_ts = 0
        unsigned int entries = 1
        bint first_element = True
    file_size += file.write(STRUCT_L.pack(block_size))
    file_size += file.write(STRUCT_Q.pack(timestamp))
    file_size += file.write(data)
    cdef:
        bytearray footer
        unsigned long bytes_to_pad
    if not use_direct_mode:
        # Pad this thing to page_size
        bytes_to_pad = page_size - (file_size % page_size)
        file.write(b'\x00' * bytes_to_pad)

        # Create a footer at the end
        footer = bytearray(page_size)
        footer[-4:] = b'\x01\x00\x00\x00'   # 1 in little endian
        file.write(footer)
    file.close()
    if gzip_compression_level:
        return DirectChunk(parent, original_path, page_size, use_descriptor_access=True,
                           gzip_compression_level=gzip_compression_level)
    else:
        if use_direct_mode:
            return DirectChunk(parent, original_path, page_size,
                               use_descriptor_access=descriptor_based_access)
        else:
            return Chunk(parent, original_path, page_size, use_descriptor_access=descriptor_based_access)

