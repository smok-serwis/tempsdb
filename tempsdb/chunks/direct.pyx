import os
import typing as tp
import struct
import warnings

from ..series cimport TimeSeries
from .gzip cimport ReadWriteGzipFile
from .base cimport Chunk


STRUCT_Q = struct.Struct('<Q')
DEF HEADER_SIZE = 4
DEF TIMESTAMP_SIZE = 8
DEF FOOTER_SIZE = 4


cdef class DirectChunk(Chunk):
    """
    Alternative implementation that extends the file as-it-goes, without allocating an entire page
    in advance.

    This is also the only chunk type capable of supporting gzip.

    Note that if you system doesn't like mmap resizing a lot, try to use it with
    `use_descriptor_access=True`.

    Note that you can only use gzip if you set use_descriptor_access to True

    :param gzip_compression_level: gzip compression level to use. 0 is default and means
        gzip disabled. If given, a warning will be emitted as gzip support is still experimental.
    :raises ValueError: non-direct descriptor was requested and gzip was enabled
    """
    def __init__(self, parent: tp.Optional[TimeSeries], path: str, page_size: int,
                 use_descriptor_access: tp.Optional[bool] = None,
                 gzip_compression_level: int = 0):
        if path.endswith('.gz'):
            warnings.warn('Please pass the path without .gz')
            path = path.replace('.gz', '')
        if path.endswith('.direct'):
            warnings.warn('Please pass the path without .direct')
            path = path.replace('.direct', '')
        if use_descriptor_access is None:
            use_descriptor_access = False
            if gzip_compression_level:
                warnings.warn('Gzip support is experimental')
                use_descriptor_access = True

        self.gzip = gzip_compression_level

        if gzip_compression_level:
            path = path + '.gz'
        else:
            path = path + '.direct'

        if gzip_compression_level:
            if not use_descriptor_access:
                raise ValueError('Use descriptor access must be enabled when using gzip')
        super().__init__(parent, path, page_size,
                         use_descriptor_access=use_descriptor_access | bool(gzip_compression_level))

    cpdef object open_file(self, str path):
        if self.gzip:
            return ReadWriteGzipFile(path, compresslevel=self.gzip)
        else:
            return super().open_file(path)

    cpdef int after_init(self) except -1:
        cdef ReadWriteGzipFile rw_gz
        if isinstance(self.file, ReadWriteGzipFile):
            rw_gz = self.file
            self.file_size = rw_gz.size
        else:
            self.file.seek(0, os.SEEK_END)
            self.file_size = self.file.tell()
        self.entries = (self.file_size - HEADER_SIZE) // self.block_size_plus
        self.pointer = self.file_size
        d = (self.file_size - self.block_size) - (self.file_size-self.block_size_plus)
        cdef bytes b = self.mmap[self.file_size-self.block_size_plus:self.file_size-self.block_size]

        self.max_ts, = STRUCT_Q.unpack(b)
        return 0

    cpdef int append(self, unsigned long long timestamp, bytes data) except -1:
        cdef bytes b
        if self.file_lock_object:
            self.file_lock_object.acquire()
        try:
            self.file_size += self.block_size_plus
            if not isinstance(self.file, ReadWriteGzipFile):
                self.file.seek(self.pointer, 0)
            b = STRUCT_Q.pack(timestamp) + data
            self.file.write(b)
            self.mmap.resize(self.file_size)
            self.pointer += self.block_size_plus
            self.entries += 1
        finally:
            if self.file_lock_object:
                self.file_lock_object.release()
        return 0

