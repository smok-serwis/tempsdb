import os
import threading

from tempsdb.exceptions import DoesNotExist, AlreadyExists
from .series cimport TimeSeries, create_series


cdef class Database:
    """
    A basic TempsDB object.

    :param path: path to the directory with the database

    :ivar path: path to  the directory with the database (str)
    """
    def __init__(self, path: str):
        self.path = path
        self.closed = False
        self.open_series = {}
        self.lock = threading.Lock()
        self.mpm = None

    cpdef TimeSeries get_series(self, name: str):
        """
        Load and return an existing series
        
        :param name: name of the series
        :type name: str
        :return: a loaded time series
        :rtype: TimeSeries
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
                self.open_series[name] = result = TimeSeries(path)
                if self.mpm is not None:
                    result.register_memory_pressure_manager(self.mpm)
        return result

    cpdef TimeSeries create_series(self, str name, int block_size,
                                   unsigned long entries_per_chunk,
                                   int page_size=4096):
        """
        Create a new series
        
        :param name: name of the series
        :type name: str
        :param block_size: size of the data field
        :type block_size: int
        :param entries_per_chunk: entries per chunk file
        :type entries_per_chunk: int
        :param page_size: size of a single page
        :type page_size: int
        :return: new series
        :rtype: TimeSeries
        :raises AlreadyExists: series with given name already exists
        """
        if os.path.isdir(os.path.join(self.path, name)):
            raise AlreadyExists('Series already exists')
        cdef TimeSeries series = create_series(os.path.join(self.name, name),
                                               block_size,
                                               entries_per_chunk, page_size=page_size)
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
            if not series.closed:
                series.register_memory_pressure_manager(mpm)
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
        for series in self.open_series.values():
            series.close()
        self.closed = True
        return 0


cpdef Database create_database(str path):
    """
    Creates a new, empty database
    
    :param path: path where the DB directory will be put
    :type path: str
    :return: a Database object
    :rtype: Database
    :raises AlreadyExists: the directory exists
    """
    if os.path.exists(path):
        raise AlreadyExists('directory already exists')
    os.mkdir(path)
    return Database(path)
