import os
import shutil
import typing as tp
import struct
import warnings

from .chunks.base cimport Chunk
from .exceptions import Corruption, AlreadyExists, StillOpen
from .iterators cimport Iterator
from tempsdb.series cimport TimeSeries, create_series


cdef class VarlenEntry:
    """
    An object representing the value.

    It is preferred for an proxy to exist, instead of copying data.
    This serves make tempsdb far more zero-copy, but it's worth it only if your
    values are routinely longer than 20-40 bytes.

    This behaves as a bytes object, in particular it can be sliced, iterated,
    and it's length obtained. It also overrides __bytes__. It's also directly comparable
    and hashable, and boolable.

    This acquires a reference to the chunk it refers, and releases it upon destruction.

    Once :meth:`~tempsdb.varlen.VarlenEntry.to_bytes` is called, it's result will be
    cached.
    """
    def __init__(self, parent: VarlenSeries, chunks: tp.List[Chunk],
                 item_no: tp.List[int]):
        self.parent = parent
        self.item_no = item_no
        cdef Chunk chunk
        self.chunks = []
        for chunk in chunks:
            if chunk is not None:
                chunk.incref()
                self.chunks.append(chunk)
        self.data = None        #: cached data, filled in by to_bytes
        self.len = -1

    cpdef bint startswith(self, bytes v):
        """
        Check whether this sequence starts with provided bytes.
        
        This will run faster than `bytes(v).startswith(b'test')` since it will
        fetch only the required amount of bytes.

        :param v: bytes to check
        :return: whether the sequence starts with provided bytes
        """
        if self.data is not None:
            return self.data.startswith(v)

        if len(v) > self.length():
            return False

        cdef bytes b = self.slice(0, self.len)
        return b == v

    cpdef bint endswith(self, bytes v):
        """
        Check whether this sequence ends with provided bytes.
        
        This will run faster than `bytes(v).endswith(b'test')` since it will
        fetch only the required amount of bytes.
        
        :param v: bytes to check
        :return: whether the sequence ends with provided bytes
        """
        if self.data is not None:
            return self.data.endswith(v)

        cdef int len_v = len(v)

        if self.len > -1:
            if len_v > self.len:
                return False
        else:
            if len_v > self.length():
                return False

        cdef bytes b = self.slice(self.len-len_v, self.len)
        return b == v

    def __gt__(self, other) -> bool:
        return self.to_bytes() > other

    def __le__(self, other) -> bool:
        return self.to_bytes() < other

    def __eq__(self, other) -> bool:
        return self.to_bytes() == other

    def __hash__(self) -> bool:
        return hash(self.to_bytes())

    def __bool__(self) -> bool:
        if self.data is not None:
            return bool(self.data)
        return bool(self.length())

    cpdef unsigned long long timestamp(self):
        """
        :return: timestamp assigned to this entry
        """
        return self.chunks[0].get_timestamp_at(self.item_no[0])

    cpdef int length(self):
        """
        :return: self length
        """
        if self.len > -1:
            return self.len
        cdef bytes b = self.chunks[0].get_slice_of_piece_at(self.item_no[0], 0, self.parent.size_field)
        assert len(b) == self.parent.size_field, 'Invalid slice!'
        self.len = self.parent.size_struct.unpack(b)[0]
        return self.len

    def __contains__(self, item: bytes) -> bool:
        return item in self.to_bytes()

    def __getitem__(self, item: tp.Union[int, slice]) -> tp.Union[int, bytes]:
        if isinstance(item, slice):
            return self.slice(item.start, item.stop)
        else:
            return self.get_byte_at(item)

    cpdef int get_byte_at(self, int index) except -1:
        """
        Return a byte at a particular index
        
        :param index: index of the byte
        :return: the value of the byte
        :raises ValueError: index too large
        """
        cdef:
            int pointer = 0
            int segment = 0
            int seg_len = 0
            int offset = self.parent.size_field
        if self.data is not None:
            return self.data[index]
        while pointer < index and segment < len(self.chunks):
            seg_len = self.parent.get_length_for(segment)
            if seg_len+pointer > index:
                return self.chunks[segment].get_byte_of_piece(self.item_no[segment],
                                                              offset+index-pointer)
            pointer += seg_len
            segment += 1
            offset = 0
        raise ValueError('Index too large')

    cpdef bytes slice(self, int start, int stop):
        """
        Returns a slice of the entry
        
        :param start: position to start at
        :param stop: position to stop at
        :return: a slice of this entry
        :raises ValueError: stop was smaller than start or indices were invalid
        """
        if stop < start:
            raise ValueError('stop smaller than start')
        if stop == start:
            return b''
        if self.data is not None:
            return self.data[start:stop]

        cdef:
            int length = stop-start
            bytearray b = bytearray(length)
            int segment = 0
            int pointer = 0
            int next_chunk_len
            int start_reading_at

        # Track down the right segment to start the read
        while pointer < start:
            next_chunk_len = self.parent.get_length_for(segment)
            if next_chunk_len > start-pointer:
                start_reading_at = start - pointer
                break
            pointer += next_chunk_len
            segment += 1

        cdef:
            int write_pointer = 0
            int chunk_len = self.parent.get_length_for(segment)
            int len_to_read = self.parent.get_length_for(segment) - start_reading_at
            Chunk chunk = self.chunks[segment]
            bytes temp_data
            int offset = self.parent.size_field
        while write_pointer < length and len(self.chunks) < segment:
            if chunk_len-start_reading_at >= + (length - write_pointer):
                # We have all the data that we require
                b[write_pointer:length] = chunk.get_slice_of_piece_at(self.item_no[segment],
                                                                      offset, offset+length-write_pointer)
                return bytes(b)

            temp_data = chunk.get_slice_of_piece_at(self.item_no[segment], 0, chunk_len)
            b[write_pointer:write_pointer+chunk_len] = temp_data
            write_pointer += chunk_len
            segment += 1
            start_reading_at = 0
            offset = 0

        raise ValueError('invalid indices')

    cpdef bytes to_bytes(self):
        """
        :return: value as bytes
        """
        if self.data is not None:
            return self.data

        cdef:
            int length = self.length()
            bytearray b = bytearray(length)
            int pointer = 0
            int segment = 0
            bytes cur_data
            int cur_data_len
            int offset = self.parent.size_field
        while pointer < length and segment < len(self.chunks):
            cur_data = self.chunks[segment].get_value_at(self.item_no[segment])
            cur_data_len = self.parent.get_length_for(segment)
            if cur_data_len > length-pointer:
                b[pointer:length] = cur_data[offset:length-pointer+offset]
                break
            b[pointer:pointer+cur_data_len] = cur_data[offset:cur_data_len+offset]
            pointer += cur_data_len
            segment += 1
            offset = 0
            first_segment = False
        self.data = bytes(b)
        self.len = length
        return self.data

    def __iter__(self):
        return iter(self.to_bytes())

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def __len__(self) -> int:
        return self.length()

    cpdef int close(self) except -1:
        """
        Close this object and release all the references.
        
        It is not necessary to call, since the destructor will call this.
        
        Do not let your VarlenEntries outlive the iterator itself!
        It will be impossible to close the iterator.
        """
        cdef Chunk chunk
        if self.chunks is None:
            return 0
        for chunk in self.chunks:
            chunk.decref()
        self.chunks = None
        return 0

    def __del__(self) -> None:
        self.close()


STRUCT_L = struct.Struct('<L')
class ThreeByteStruct:
    __slots__ = ()
    def pack(self, v: int) -> bytes:
        return STRUCT_L.pack(v)[0:3]

    def unpack(self, v: bytes) -> tp.Tuple[int]:
        return STRUCT_L.unpack(v+b'\x00')


cdef class VarlenIterator:
    """
    A result of a varlen series query.

    This iterator will close itself when completed. If you break out of it's
    iteration, please close it youself via
    :meth:`~tempsdb.varlen.VarlenIterator.close`

    If you forget to do that, a warning will be issued and the destructor will
    close it automatically.

    Also supports the context manager syntax of :class:`~tempsdb.iterators.Iterator`

    :param parent: parent series
    :param start: started series
    :param stop: stopped series
    :param direct_bytes: whether to iterate with bytes values instead of
        :class:`~tempsdb.varlen.VarlenEntry`. Note that setting this to True
        will result in a performance drop, since it will copy, but it should
        be faster if your typical entry is less than 20 bytes.

    :ivar name: series' name (str)
    :ivar path: path to series' directory (str)
    :ivar max_entries_per_chunk: maximum entries per chunk (int)
    :ivar length_profile: length profile (tp.List[int])
    """
    def __init__(self, parent: VarlenSeries, start: int, stop: int,
                 direct_bytes: bool = False):
        self.parent = parent
        self.parent.references += 1
        self.start = start
        self.stop = stop
        self.direct_bytes = direct_bytes
        self.closed = False
        cdef int amount_series = len(self.parent.series)
        self.positions = [None] * amount_series
        self.chunks = [None] * amount_series
        self.timestamps = [None] * amount_series
        self.iterators = []
        cdef:
            TimeSeries series
            Iterator iterator
            Chunk chunk
            unsigned int pos
            unsigned long long ts
            tuple tpl
            int i
        for i in range(amount_series):
            iterator = self.parent.series[i].iterate_range(start, stop)
            self.iterators.append(iterator)
        for i in range(amount_series):
            iterator = self.iterators[i]
            self.advance_series(i, True)

    cdef int advance_series(self, int index, bint force) except -1:
        cdef:
            Iterator iterator = self.iterators[index]
            tuple tpl
            Chunk old_chunk, chunk
        if iterator is None and not force:
            return 0

        tpl = iterator.next_item_pos()
        if tpl is None:
            self.timestamps[index] = None
            self.positions[index] = None
            old_chunk = self.chunks[index]
            if old_chunk is not None:
                old_chunk.decref()
            self.chunks[index] = None
            iterator.close()
            self.iterators[index] = None
        else:
            ts, pos, chunk = tpl
            self.timestamps[index] = ts
            self.positions[index] = pos
            self.chunks[index] = chunk
        return 0

    cpdef VarlenEntry get_next(self):
        """
        Return next element of the iterator, or None if no more available.
        """
        if self.timestamps[0] is None:
            return None
        cdef:
            unsigned long long ts = self.timestamps[0]
            list chunks = []
            list positions = []
            int i

        for i in range(len(self.chunks)):
            if self.timestamps[i] is None:
                break
            elif self.timestamps[i] == ts:
                chunks.append(self.chunks[i])
                positions.append(self.positions[i])
                self.advance_series(i, False)

        return VarlenEntry(self.parent, chunks, positions)

    def __next__(self):
        cdef VarlenEntry varlen_entry = self.get_next()
        if varlen_entry is None:
            self.close()
            raise StopIteration('iterator exhausted')
        else:
            if self.direct_bytes:
                return varlen_entry.timestamp(), varlen_entry.to_bytes()
            else:
                return varlen_entry.timestamp(), varlen_entry

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        return self

    cpdef int close(self) except -1:
        """
        Close this iterator and release all the resources
        
        No-op if already closed.
        """
        cdef:
            Iterator iterator
            Chunk chunk
        if self.closed:
            return 0
        self.closed = True
        for iterator in self.iterators:
            if iterator is not None:
                iterator.close()
        for chunk in self.chunks:
            if chunk is not None:
                chunk.decref()
        self.parent.references -= 1
        return 0

    def __del__(self):
        if not self.closed:
            warnings.warn('You forgot to close a VarlenIterator. Please close them explicitly!')
            self.close()


cdef class VarlenSeries:
    """
    A time series housing variable length data.

    It does that by splitting the data into chunks and encoding them in multiple
    series.

    :param path: path to directory containing the series
    :param name: name of the series
    """
    cdef void register_memory_pressure_manager(self, object mpm):
        self.mpm = mpm
        cdef TimeSeries series
        for series in self.series:
            series.register_memory_pressure_manager(mpm)

    cpdef int close_chunks(self) except -1:
        """
        Close unnecessary chunks
        """
        cdef TimeSeries series
        for series in self.series:
            series.close_chunks()
        return 0

    @property
    def last_entry_ts(self) -> int:
        """
        :return: last entry's timestamp, or barring that a 0
        """
        try:
            return self.get_current_value()[0]
        except ValueError:
            return 0

    cpdef VarlenIterator iterate_range(self, unsigned long long start, unsigned long long stop,
                                       bint direct_bytes=False):
        """
        Return an iterator with the data
        
        :param start: timestamp to start iterating
        :param stop: timestamp to stop iterating at
        :param direct_bytes: whether to return a tuple of (int, bytes) instead of 
            (int, :class:`~tempsdb.varlen.VarlenEntry`)
        :return: a :class:`~tempsdb.varlen.VarlenIterator` instance
        """
        return VarlenIterator(self, start, stop, direct_bytes=direct_bytes)

    cpdef tuple get_current_value(self):
        """
        Return latest value of this series
                        
        :return: tuple of (timestamp, value)
        :rtype: tp.Tuple[int, bytes]
        :raises ValueError: series has no data
        """
        if self.root_series.last_chunk is None:
            raise ValueError('No data in series')
        cdef:
            VarlenIterator it = self.iterate_range(self.root_series.last_entry_ts,
                                             self.root_series.last_entry_ts)
            VarlenEntry et = it.get_next()
        try:
            return et.timestamp(), et.to_bytes()
        finally:
            et.close()
            it.close()

    def __init__(self, str path, str name, bint use_descriptor_based_access = False):
        self.closed = False
        self.mmap_enabled = not use_descriptor_based_access
        self.path = path
        self.references = 0
        self.mpm = None
        self.name = name
        self.root_series = TimeSeries(os.path.join(path, 'root'), 'root',
                                      use_descriptor_based_access=not self.mmap_enabled)
        self.gzip_level = self.root_series.gzip_level
        self.max_entries_per_chunk = self.root_series.max_entries_per_chunk
        try:
            self.size_field = self.root_series.metadata['size_field']
            self.length_profile = self.root_series.metadata['length_profile']
        except (KeyError, TypeError):
            raise Corruption('required keys not present or invalid in root subseries')

        if self.size_field == 1:
            self.size_struct = struct.Struct('<B')
        elif self.size_field == 2:
            self.size_struct = struct.Struct('<H')
        elif self.size_field == 3:
            self.size_struct = ThreeByteStruct()
        elif self.size_field == 4:
            self.size_struct = struct.Struct('<L')
        else:
            self.root_series.close()
            raise Corruption('Invalid size_field!')

        cdef:
            list sub_series = []
            str dir_name
        for dir_name in os.listdir(path):
            if dir_name != 'root':
                sub_series.append(dir_name)

        try:
            sub_series.sort(key=lambda x: int(x))
        except ValueError:
            raise Corruption('Invalid directory name')

        cdef:
            int i = 1
            int tot_length = self.length_profile[0]
        self.series = [self.root_series]
        for dir_name in sub_series:
            tot_length += self.get_length_for(i)
            i += 1
            self.series.append(TimeSeries(os.path.join(path, dir_name), dir_name,
                                          use_descriptor_based_access=not self.mmap_enabled))

        self.current_maximum_length = tot_length

    cpdef int enable_mmap(self) except -1:
        """
        Enable using mmap for these series
        """
        self.mmap_enabled = True
        cdef TimeSeries series
        for series in self.series:
            series.enable_mmap()
        return 0

    cpdef int disable_mmap(self) except -1:
        """
        Disable using mmap for these series
        """
        self.mmap_enabled = False
        cdef TimeSeries series
        for series in self.series:
            series.disable_mmap()
        return 0

    cpdef int mark_synced_up_to(self, unsigned long long timestamp) except -1:
        """
        Mark the series as synchronized up to particular period
        
        :param timestamp: timestamp of synchronization
        """
        self.root_series.mark_synced_up_to(timestamp)
        return 0

    @property
    def last_entry_synced(self) -> int:
        """
        :return: timestamp of the last entry synchronized. Starting value is 0
        """
        return self.root_series.last_entry_synced

    cpdef int append(self, unsigned long long timestamp, bytes data) except -1:
        """
        Append an entry to the series
        
        :param timestamp: timestamp to append it with
        :param data: data to write
        
        :raises ValueError: too long an entry
        """
        cdef int data_len = len(data)
        if data_len > self.get_maximum_length():
            raise ValueError('data too long')
        if data_len < self.get_length_for(0):
            data = self.size_struct.pack(len(data)) + data
            self.root_series.append_padded(timestamp, data)
            return 0

        while self.current_maximum_length < data_len:
            self.add_series()

        # At this point data is too large to be put in a single series
        cdef:
            bytes data_to_put = self.size_struct.pack(len(data)) + data[:self.get_length_for(0)]
            int pointer = self.get_length_for(0)
            int segment = 1
            int cur_len
        self.root_series.append(timestamp, data_to_put)
        while pointer < len(data) and segment < len(self.series):
            cur_len = self.get_length_for(segment)
            data_to_put = data[pointer:pointer+cur_len]
            self.series[segment].append_padded(timestamp, data_to_put)
            pointer += cur_len
            segment += 1
        return 0

    cpdef int delete(self) except -1:
        """
        Erases this variable length series from the disk.
        
        Closes this series as a side-effect.
        """
        self.close()
        shutil.rmtree(self.path)
        return 0

    cdef int add_series(self) except -1:
        """
        Creates a new series to hold part of ours data 
        
        Updates :attr:`~tempsdb.varlen.VarlenSeries.current_maximum_length`.
        """
        from .series import create_series

        cdef:
            int new_name = len(self.series)
            int new_len = self.get_length_for(new_name)
            str new_name_s = str(new_name)
            TimeSeries series = create_series(os.path.join(self.path, new_name_s),
                                              new_name_s,
                                              new_len,
                                              self.max_entries_per_chunk,
                                              use_descriptor_based_access=not self.mmap_enabled,
                                              gzip_level=self.gzip_level)
        if self.mpm is not None:
            series.register_memory_pressure_manager(self.mpm)
        self.series.append(series)
        self.current_maximum_length += new_len

    cdef int get_length_for(self, int index):
        """
        Get the length of the time series at a particular index.
        
        :param index: index of the time series, numbered from 0
        """
        return self.length_profile[-1 if index >= len(self.length_profile) else index]

    cpdef unsigned long open_chunks_mmap_size(self):
        """
        :return: total area of memory taken by mmaps, in bytes
        """
        cdef:
            unsigned long long total = 0
            TimeSeries series
        for series in self.series:
            total += series.open_chunks_mmap_size()
        return total

    cpdef int close(self, bint force=False) except -1:
        """
        Close this series.
        
        No-op if already closed.
        
        :param force: set to True to ignore open references
        
        :raises StillOpen: some references are being held
        """
        if self.closed:
            return 0

        if self.references and not force:
            raise StillOpen('still some iterators around')

        self.closed = True
        cdef TimeSeries series
        for series in self.series:
            series.close()
        return 0

    cpdef int trim(self, unsigned long long timestamp) except -1:
        """
        Try to delete all entries younger than timestamp
        
        :param timestamp: timestamp that separates alive entries from the dead
        """
        cdef TimeSeries series
        for series in self.series:
            series.trim(timestamp)
        return 0

    cpdef long long get_maximum_length(self) except -1:
        """
        :return: maximum length of an element capable of being stored in this series
        """
        if self.size_field == 1:
            return 0xFF
        elif self.size_field == 2:
            return 0xFFFF
        elif self.size_field == 3:
            return 0xFFFFFF
        elif self.size_field == 4:
            return 0x7FFFFFFF
        else:
            raise ValueError('How did this happen?')

from tempsdb.series cimport TimeSeries

cpdef VarlenSeries create_varlen_series(str path, str name, int size_struct, list length_profile,
                                        int max_entries_per_chunk,
                                        bint use_descriptor_based_access=False,
                                        int gzip_level=0):
    """
    Create a variable length series
    
    :param path: path where the directory will be placed
    :param name: name of the series
    :param size_struct: size of the length indicator. Must be one of 1, 2, 3 or 4.
    :param length_profile: series' length profile
    :param max_entries_per_chunk: maximum entries per a chunk file
    :param use_descriptor_based_access: whether to disable mmap
    :param gzip_level: level of gzip compression. Leave at 0 (default) to disable compression.
    :return: newly created VarlenSeries
    :raises AlreadyExists: directory exists at given path
    :raises ValueError: invalid length profile or max_entries_per_chunk or size_struct
    """
    from tempsdb.series import create_series

    if os.path.exists(path):
        raise AlreadyExists('directory present at paht')
    if not length_profile or not max_entries_per_chunk:
        raise ValueError('invalid parameter')
    if not (1 <= size_struct <= 4):
        raise ValueError('invalid size_struct')

    os.mkdir(path)
    cdef TimeSeries root_series = create_series(os.path.join(path, 'root'),
                                                'root',
                                                size_struct+length_profile[0],
                                                max_entries_per_chunk,
                                                use_descriptor_based_access=use_descriptor_based_access,
                                                gzip_level=gzip_level)
    root_series.set_metadata({'size_field': size_struct,
                              'length_profile': length_profile})
    root_series.close()
    return VarlenSeries(path, name)
