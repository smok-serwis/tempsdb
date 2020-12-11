import os
import struct

from tempsdb.exceptions import Corruption, AlreadyExists
from .series cimport TimeSeries, create_series


cdef class VarlenSeries:
    """
    A time series housing variable length data.

    It does that by splitting the data into chunks and encoding them in multiple
    series.

    :param path: path to directory containing the series
    :param name: name of the series
    """
    def __init__(self, path: str, name: str):
        self.closed = False
        self.path = path
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
        """
        cdef int data_len = len(data)
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

    cpdef int add_series(self) except -1:
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

    cpdef int get_length_for(self, int index):
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


cpdef VarlenSeries create_varlen_series(str path, str name, int size_struct, list length_profile,
                                        int max_entries_per_chunk):
    """
    Create a variable length series
    
    :raises AlreadyExists: directory exists at given path
    :raises ValueError: invalid length profile or max_entries_per_chunk
    """
    if os.path.exists(path):
        raise AlreadyExists('directory present at paht')
    if not length_profile or not max_entries_per_chunk:
        raise ValueError('invalid parameter')

    os.mkdir(path)
    cdef TimeSeries root_series = create_series(os.path.join(path, 'root'),
                                                'root',
                                                size_struct+length_profile[0],
                                                max_entries_per_chunk)
    root_series.set_metadata({'size_field': size_struct,
                              'length_profile': length_profile})
    root_series.close()
    return VarlenSeries(path, name)
