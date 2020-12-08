import os
import threading

from satella.coding import DictDeleter

from tempsdb.exceptions import DoesNotExist, AlreadyExists
from .series cimport TimeSeries, create_series


cdef class Database:
    """
    A basic TempsDB object.

    :param path: path to the directory with the database
    :raises DoesNotExist: database does not exist, use `create_database`

    :ivar path: path to  the directory with the database (str)
    """
    def __init__(self, path: str):
        if not os.path.isdir(path):
            raise DoesNotExist('Database does not exist')
        self.path = path
        self.closed = False
        self.open_series = {}
        self.lock = threading.Lock()
        self.mpm = None

    cpdef list get_open_series(self):
        """
        Return all open series
        
        :return: open series
        :rtype: tp.List[TimeSeries]
        """
        cdef:
            list output = []
            TimeSeries series
            str name
        with self.lock:
            with DictDeleter(self.open_series) as dd:
                for series in dd.values():
                    if series.closed:
                        dd.delete()
                    else:
                        output.append(series)
        return output

    cpdef TimeSeries get_series(self, name: str, bint use_descriptor_based_access = False):
        """
        Load and return an existing series
        
        :param name: name of the series
        
        :param use_descriptor_based_access: whether to use descriptor based access instead of mmap, 
            default is False
        :return: a loaded time series
        :raises DoesNotExist: series does not exist
        """
        cdef:
            TimeSeries result
            str path
        if name in self.open_series:
            result = self.open_series[name]
        else:
            path = os.path.join(self.path, name)
            with self.lock:
                # Check a second time due to the lock
                if name in self.open_series:
                    if self.open_series[name].closed:
                        del self.open_series[name]
                        return self.open_series(name)
                    return self.open_series[name]
                if not os.path.isdir(path):
                    raise DoesNotExist('series %s does not exist' % (name, ))
                self.open_series[name] = result = TimeSeries(path, name,
                                                             use_descriptor_based_access=use_descriptor_based_access)
                if self.mpm is not None:
                    result.register_memory_pressure_manager(self.mpm)
        return result

    cpdef int close_all_open_series(self) except -1:
        """
        Closes all open series
        """
        cdef TimeSeries series
        with self.lock:
            for series in self.open_series.values():
                series.close()
            self.open_series = {}
        return 0

    cpdef unsigned long long get_first_entry_for(self, str name):
        """
        Get first timestamp stored in a particular series without opening it
                        
        :param name: series name
        :return: first timestamp stored in this series
        :raises DoesNotExist: series does not exist
        :raises ValueError: timestamp does not have any data
        """
        cdef str path = os.path.join(self.path, name)
        if not os.path.isdir(path):
            raise DoesNotExist('series does not exist')
        cdef:
            unsigned long long minimum_ts = 0xFFFFFFFFFFFFFFFF
            list files = os.listdir(path)
            unsigned long long candidate_ts
        if len(files) == 1:
            raise ValueError('Timestamp does not have any data')
        for name in files:
            try:
                candidate_ts = int(name)
            except ValueError:
                continue
            if candidate_ts < minimum_ts:
                minimum_ts = candidate_ts
        return minimum_ts

    cpdef int sync(self) except -1:
        """
        Synchronize all the data with the disk
        """
        cdef TimeSeries series
        for series in self.open_series.values():
            if not series.closed:
                series.sync()

    cpdef list get_all_series(self):
        """
        Stream all series available within this database
                
        :return: a list of series names
        :rtype: tp.List[str]
        """
        return os.listdir(self.path)

    cpdef TimeSeries create_series(self, str name, int block_size,
                                   unsigned long entries_per_chunk,
                                   int page_size=4096,
                                   bint use_descriptor_based_access=False):
        """
        Create a new series
        
        :param name: name of the series
        :param block_size: size of the data field
        :param entries_per_chunk: entries per chunk file
        :param page_size: size of a single page. Default is 4096
        :param use_descriptor_based_access: whether to use descriptor based access instead of mmap.
            Default is False
        :return: new series
        :raises ValueError: block size was larger than page_size plus a timestamp
        :raises AlreadyExists: series with given name already exists
        """
        if block_size > page_size + 8:
            raise ValueError('Invalid block size, pick larger page')
        if os.path.isdir(os.path.join(self.path, name)):
            raise AlreadyExists('Series already exists')
        cdef TimeSeries series = create_series(os.path.join(self.path, name), name,
                                               block_size,
                                               entries_per_chunk, page_size=page_size,
                                               use_descriptor_based_access=use_descriptor_based_access)
        self.open_series[name] = series
        return series

    cpdef int register_memory_pressure_manager(self, object mpm) except -1:
        """
        Register a satella MemoryPressureManager_ to close chunks if low on memory.
        
        .. _MemoryPressureManager: https://satella.readthedocs.io/en/latest/instrumentation/memory.html
        
        :param mpm: MemoryPressureManager to use
        :type mpm: satella.instrumentation.memory.MemoryPressureManager
        """
        self.mpm = mpm
        cdef TimeSeries series
        for series in self.open_series.values():
            series.register_memory_pressure_manager(mpm)    # no-op if already closed
        return 0

    def __del__(self):
        self.close()

    cpdef int close(self) except -1:
        """
        Close this TempsDB database
        """
        if self.closed:
            return 0
        cdef TimeSeries series
        with self.lock:
            for series in self.open_series.values():
                series.close()  # because already closed series won't close themselves
            self.open_series = {}
        self.closed = True
        return 0


cpdef Database create_database(str path):
    """
    Creates a new, empty database
    
    :param path: path where the DB directory will be put
    :return: a Database object
    :raises AlreadyExists: the directory exists
    """
    if os.path.exists(path):
        raise AlreadyExists('directory already exists')
    os.mkdir(path)
    return Database(path)
