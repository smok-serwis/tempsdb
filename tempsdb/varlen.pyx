import os
import shutil
import typing as tp
import struct
import warnings

from .chunks cimport Chunk
from .exceptions import Corruption, AlreadyExists
from .iterators cimport Iterator
from .series cimport TimeSeries, create_series


cdef class VarlenEntry:
    """
    An object representing the value.

    It is preferred for an proxy to exist, instead of copying data.
    This serves make tempsdb far more zero-copy, but it's worth it only if your
    values are routinely longer than 20-40 bytes.

    This behaves as a bytes object, in particular it can be sliced, iterated,
    and it's length obtained. It also overloads __bytes__.

    Once :meth:`~tempsdb.varlen.VarlenEntry.to_bytes` is called, it's result will be
    cached.
    """
    def __init__(self, parent: VarlenSeries, chunks: tp.List[Chunk],
                 item_no: tp.List[int]):
        self.parent = parent
        self.item_no = item_no
        self.chunks = chunks
        self.data = None        #: cached data, filled in by to_bytes

    cpdef unsigned long long timestamp(self):
        """
        :return: timestamp assigned to this entry
        """
        return self.chunks[0].get_timestamp_at(self.item_no[0])

    cpdef int length(self):
        """
        :return: self length
        """
        if self.data is not None:
            return len(self.data)
        cdef bytes b = self.chunks[0].get_slice_of_piece_at(self.item_no[0], 0, self.parent.size_field)
        b = b[:self.parent.size_field]
        return self.parent.size_struct.unpack(b)[0]

    def __contains__(self, item: bytes) -> bool:
        return item in self.to_bytes()

    def __getitem__(self, item):
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
        if self.data is not None:
            return self.data[index]
        while pointer < index and segment < len(self.chunks):
            seg_len = self.parent.get_length_for(segment)
            if seg_len+pointer > index:
                return self.chunks[segment].get_byte_of_piece(self.item_no[segment],
                                                              index-pointer)
            pointer += seg_len
            segment += 1
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
        while write_pointer < length and len(self.chunks) < segment:
            if chunk_len-start_reading_at >= + (length - write_pointer):
                # We have all the data that we require
                b[write_pointer:length] = chunk.get_slice_of_piece_at(self.item_no[segment],
                                                                      0, length-write_pointer)
                return bytes(b)

            temp_data = chunk.get_slice_of_piece_at(self.item_no[segment], 0, chunk_len)
            b[write_pointer:write_pointer+chunk_len] = temp_data
            write_pointer += chunk_len
            segment += 1
            start_reading_at = 0

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
        while pointer < length:
            cur_data = self.chunks[segment].get_piece_at(self.item_no[segment])[1]
            cur_data_len = len(cur_data)
            if cur_data_len > length-pointer:
                b[pointer:length] = cur_data[:cur_data_len-(length-pointer)]
            else:
                b[pointer:pointer+cur_data_len] = cur_data
            pointer += cur_data_len
            segment += 1
        if self.data is None:
            self.data = bytes(b)
        return self.data

    def __iter__(self):
        return iter(self.to_bytes())

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def __len__(self) -> int:
        return self.length()


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

    Please close it when you're done.
    If you forget to do that, a warning will be issued and the destructor will
    close it automatically.

    :param parent: parent series
    :param start: started series
    :param stop: stopped series
    :param direct_bytes: whether to iterate with bytes values instead of
        :class:`~tempsdb.varlen.VarlenEntry`. Note that setting this to True
        will result in a performance drop, since it will copy, but it should
        be faster if your typical entry is less than 20 bytes.
    """
    def __init__(self, parent: VarlenSeries, start: int, stop: int,
                 direct_bytes: bool = False):
        self.parent = parent
        self.start = start
        self.stop = stop
        self.direct_bytes = direct_bytes
        self.closed = False
        self.chunk_positions = []
        self.iterators = []
        self.next_timestamps = []
        cdef:
            TimeSeries series
            Iterator iterator
        for series in self.parent.series:
            iterator = series.iterate_range(start, stop)
            iterator.get_next()
            self.iterators.append(iterator)
            self.chunk_positions.append(iterator.i)


    def __next__(self) -> tp.Tuple[int, tp.Union[bytes, VarlenEntry]]:
        ...

    def __iter__(self):
        return self

    cpdef int close(self) except -1:
        """
        Close this iterator and release all the resources
        """
        cdef Iterator iterator
        if not self.closed:
            self.parent.references -= 1
            self.closed = True
            for iterator in self.iterators:
                iterator.close()
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

    def __init__(self, path: str, name: str):
        self.closed = False
        self.path = path
        self.references = 0
        self.mpm = None
        self.name = name
        self.root_series = TimeSeries(os.path.join(path, 'root'), 'root')
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
            self.series.append(TimeSeries(os.path.join(path, dir_name), dir_name))

        self.current_maximum_length = tot_length

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
        while pointer < len(data):
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

    cdef int add_series(self) except -1:
        """
        Creates a new series to hold part of ours data 
        
        Updates :attr:`~tempsdb.varlen.VarlenSeries.current_maximum_length`.
        """
        cdef:
            int new_name = len(self.series)
            int new_len = self.get_length_for(new_name)
            str new_name_s = str(new_name)
            TimeSeries series = create_series(os.path.join(self.path, new_name_s),
                                              new_name_s,
                                              new_len,
                                              self.max_entries_per_chunk)
        self.series.append(series)
        self.current_maximum_length += new_len

    cdef int get_length_for(self, int index):
        """
        Get the length of the time series at a particular index.
        
        :param index: index of the time series, numbered from 0
        """
        return self.length_profile[-1 if index >= len(self.length_profile) else index]

    cpdef int close(self) except -1:
        """
        Close this series
        """
        if self.closed:
            return 0

        self.closed = True
        cdef TimeSeries series
        for series in self.series:
            series.close()

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
            return 0xFFFFFFFF
        else:
            raise ValueError('How did this happen?')


cpdef VarlenSeries create_varlen_series(str path, str name, int size_struct, list length_profile,
                                        int max_entries_per_chunk):
    """
    Create a variable length series
    
    :param path: path where the directory will be placed
    :param name: name of the series
    :param size_struct: size of the length indicator. Must be one of 1, 2, 3 or 4.
    :param length_profile: series' length profile
    :param max_entries_per_chunk: maximum entries per a chunk file
    :return: newly created VarlenSeries
    :raises AlreadyExists: directory exists at given path
    :raises ValueError: invalid length profile or max_entries_per_chunk or size_struct
    """
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
                                                max_entries_per_chunk)
    root_series.set_metadata({'size_field': size_struct,
                              'length_profile': length_profile})
    root_series.close()
    return VarlenSeries(path, name)
