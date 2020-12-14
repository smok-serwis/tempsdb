import io
import os
import threading
import typing as tp
import struct
import mmap
import warnings

from ..exceptions import Corruption, InvalidState, AlreadyExists, StillOpen
from ..series cimport TimeSeries

DEF HEADER_SIZE = 4
DEF TIMESTAMP_SIZE = 8
DEF FOOTER_SIZE = 4
STRUCT_Q = struct.Struct('<Q')
STRUCT_L = struct.Struct('<L')
STRUCT_LQ = struct.Struct('<LQ')


cdef class AlternativeMMap:
    """
    An alternative mmap implementation used when mmap cannot allocate due to memory issues
    """
    def flush(self):
        self.io.flush()

    def madvise(self, a, b, c):
        ...

    def resize(self, int file_size):
        self.size = file_size

    def __init__(self, io_file: io.BinaryIO, file_lock_object):
        self.io = io_file
        self.io.seek(0, io.SEEK_END)
        self.size = self.io.tell()
        self.file_lock_object = file_lock_object

    def __getitem__(self, item: tp.Union[int, slice]) -> tp.Union[int, bytes]:
        cdef:
            unsigned long start = item.start
            unsigned long stop = item.stop
            bytes b
        with self.file_lock_object:
            if isinstance(item, int):
                self.io.seek(item, 0)
                b = self.io.read(1)
                return b[0]
            else:
                start = item.start
                stop = item.stop
                self.io.seek(start, 0)
                return self.io.read(stop-start)

    def __setitem__(self, key: tp.Union[int, slice], value: tp.Union[int, bytes]) -> None:
        cdef:
            unsigned long start = key.start
        if isinstance(key, int):
            self[key:key+1] = bytes([value])
        else:
            with self.file_lock_object:
                self.io.seek(start, 0)
                self.io.write(value)

    def close(self):
        pass


cdef class Chunk:
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
    cpdef unsigned long get_mmap_size(self):
        """
        :return: how many bytes are mmaped?
        :rtype: int
        """
        if isinstance(self.mmap, AlternativeMMap):
            return 0
        else:
            return self.file_size

    cpdef int switch_to_mmap_based_access(self) except -1:
        """
        Switch self to mmap-based access instead of descriptor-based.
        
        No-op if already in mmap mode.
        
        :raises Corruption: unable to mmap file due to an unrecoverable error
        """
        if isinstance(self.mmap, AlternativeMMap):
            try:
                self.mmap = mmap.mmap(self.file.fileno(), 0)
                self.file_lock_object = None
            except OSError as e:
                if e.errno in (11,      # EAGAIN - memory is too low
                               12,      # ENOMEM - no memory space available
                               19,      # ENODEV - fs does not support mmapping
                               75):     # EOVERFLOW - too many pages would have been used
                    pass
                else:
                    self.file.close()
                    self.closed = True
                    raise Corruption(f'Failed to mmap chunk file: {e}')
        return 0

    cpdef int switch_to_descriptor_based_access(self) except -1:
        """
        Switch self to descriptor-based access instead of mmap.
        
        No-op if already in descriptor based mode.
        """
        if isinstance(self.mmap, AlternativeMMap):
            return 0
        self.mmap.close()
        self.file_lock_object = threading.Lock()
        self.mmap = AlternativeMMap(self.file, self.file_lock_object)
        return 0

    def __init__(self, parent: tp.Optional[TimeSeries], path: str, page_size: int,
                 use_descriptor_access: bool = False):
        cdef bytes b
        self.file_size = os.path.getsize(path)
        self.page_size = page_size
        self.parent = parent
        self.closed = False
        self.path = path
        self.file = open(self.path, 'rb+')
        self.file_lock_object = None

        if use_descriptor_access:
            self.file_lock_object = threading.Lock()
            self.mmap = AlternativeMMap(self.file, self.file_lock_object)
        else:
            try:
                self.mmap = mmap.mmap(self.file.fileno(), 0)
            except OSError as e:
                if e.errno in (11,      # EAGAIN - memory is too low
                               12,      # ENOMEM - no memory space available
                               19,      # ENODEV - fs does not support mmapping
                               75):     # EOVERFLOW - too many pages would have been used
                    self.file_lock_object = threading.Lock()
                    self.mmap = AlternativeMMap(self.file, self.file_lock_object)
                else:
                    self.file.close()
                    self.closed = True
                    raise Corruption(f'Failed to mmap chunk file: {e}')

        try:
            self.block_size, self.min_ts = STRUCT_LQ.unpack(self.mmap[0:HEADER_SIZE+TIMESTAMP_SIZE])
            self.block_size_plus = self.block_size + TIMESTAMP_SIZE
        except struct.error:
            self.close()
            raise Corruption('Could not read the header of the chunk file %s' % (self.path, ))

        self.entries, = STRUCT_L.unpack(self.mmap[self.file_size-FOOTER_SIZE:self.file_size])
        self.pointer = self.entries*self.block_size_plus+HEADER_SIZE
        self.max_ts = self.get_timestamp_at(self.entries-1)

        if self.pointer >= self.page_size:
            # Inform the OS that we don't need the header anymore
            self.mmap.madvise(mmap.MADV_DONTNEED, 0, HEADER_SIZE+TIMESTAMP_SIZE)

    def __getitem__(self, index: tp.Union[int, slice]):
        if isinstance(index, slice):
            return self.iterate_range(index.start, index.stop)
        else:
            return self.get_piece_at(index)

    cdef int sync(self) except -1:
        """
        Synchronize the mmap
        """
        self.mmap.flush()
        return 0

    cdef int extend(self) except -1:
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

    def __iter__(self) -> tp.Iterator[tp.Tuple[int, bytes]]:
        return self._iterate(0, self.entries)

    def __len__(self):
        return self.length()

    cpdef int close(self, bint force=False) except -1:
        """
        Close the chunk and close the allocated resources
        
        :param force: whether to close the chunk even if it's open somewhere
        :raises StillOpen: this chunk has a parent attached and the parent
            says that this chunk is still being referred to
        """
        if self.closed:
            return 0
        cdef unsigned long long name = self.name()
        if self.parent:
            with self.parent.open_lock:
                if not force and self.parent.get_references_for(name) > 0:
                    raise StillOpen('this chunk is opened')
                del self.parent.refs_chunks[name]
                del self.parent.open_chunks[name]
        self.parent = None
        self.mmap.close()
        self.file.close()
        return 0

    def __del__(self) -> None:
        if self.closed:
            return
        warnings.warn('You forgot to close a Chunk')
        self.close()


cpdef Chunk create_chunk(TimeSeries parent, str path, unsigned long long timestamp,
                         bytes data, int page_size, bint descriptor_based_access=False):
    """
    Creates a new chunk on disk
    
    :param parent: parent time series
    :param path: path to the new chunk file
    :param timestamp: timestamp for first entry to contain
    :param data: data of the first entry
    :param page_size: size of a single page for storage 
    :param descriptor_based_access: whether to use descriptor based access instead of mmap. 
        Default is False
    :raises ValueError: entries in data were not of equal size, or data was empty or data
        was not sorted by timestamp or same timestamp appeared twice
    :raises AlreadyExists: chunk already exists 
    """
    if os.path.exists(path):
        raise AlreadyExists('chunk already exists!')
    if not data:
        raise ValueError('Data is empty')
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

    # Pad this thing to page_size
    cdef unsigned long bytes_to_pad = page_size - (file_size % page_size)
    file.write(b'\x00' * bytes_to_pad)

    # Create a footer at the end
    cdef bytearray footer = bytearray(page_size)
    footer[-4:] = b'\x01\x00\x00\x00'   # 1 in little endian
    file.write(footer)
    file.close()
    return Chunk(parent, path, page_size, use_descriptor_access=descriptor_based_access)

