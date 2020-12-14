import io
import struct

from ..exceptions import Corruption, InvalidState, AlreadyExists, StillOpen
from .base cimport Chunk

DEF HEADER_SIZE = 4
DEF TIMESTAMP_SIZE = 8
DEF FOOTER_SIZE = 4
STRUCT_Q = struct.Struct('<Q')
STRUCT_L = struct.Struct('<L')


cdef class NormalChunk(Chunk):
    """
    Represents a single chunk of time series.

    This also implements an iterator interface, and will iterate with tp.Tuple[int, bytes],
    as well as a sequence protocol.

    This will try to mmap opened files, but if mmap fails due to not enough memory this
    will use descriptor-based access.

    :param parent: parent time series
    :param path: path to the chunk file
    :param use_descriptor_access: whether to use descriptor based access instead of mmap

    :ivar path: path to the chunk (str)
    :ivar min_ts: timestamp of the first entry stored (int)
    :ivar max_ts: timestamp of the last entry stored (int)
    :ivar block_size: size of the data entries (int)
    :ivar entries: amount of entries in this chunk (int)
    :ivar page_size: size of the page (int)
    """

    cpdef int extend(self) except -1:
        """
        Adds PAGE_SIZE bytes to this file
        """
        cdef bytearray ba
        if self.file_lock_object:
            self.file_lock_object.acquire()
        try:
            self.file_size += self.page_size
            self.file.seek(0, io.SEEK_END)
            ba = bytearray(self.page_size)
            ba[self.page_size-FOOTER_SIZE:self.page_size] = STRUCT_L.pack(self.entries)
            self.file.write(ba)
            try:
                self.mmap.resize(self.file_size)
            except OSError as e:
                if e.errno == 12:   # ENOMEM
                    self.switch_to_descriptor_based_access()
                else:
                    raise
        finally:
            if self.file_lock_object:
                self.file_lock_object.release()
        return 0

    cpdef int append(self, unsigned long long timestamp, bytes data) except -1:
        """
        Append a record to this chunk.
        
        Might range from very fast (just a memory operation) to quite slow (adding a new page
        to the file).
        
        Simultaneous writing is not thread-safe.
        
        Timestamp and data is not checked for, this is supposed to be handled by
        :class:`~tempsdb.series.TimeSeries`.
        
        :param timestamp: timestamp of the entry
        :param data: data to write
        :raises InvalidState: chunk is closed
        """
        if self.closed:
            raise InvalidState('chunk is closed')

        if self.pointer >= self.file_size-FOOTER_SIZE-self.block_size_plus:
            self.extend()
        cdef unsigned long long ptr_end = self.pointer + TIMESTAMP_SIZE
        # Append entry
        self.mmap[self.pointer:ptr_end] = STRUCT_Q.pack(timestamp)
        self.mmap[ptr_end:ptr_end+self.block_size] = data
        self.entries += 1
        # Update entries count
        self.mmap[self.file_size-FOOTER_SIZE:self.file_size] = STRUCT_L.pack(self.entries)
        # Update properties
        self.max_ts = timestamp
        self.pointer += self.block_size_plus
        return 0
