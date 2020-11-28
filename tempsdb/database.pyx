import os
import threading

from tempsdb.exceptions import DoesNotExist, AlreadyExists
from .series cimport TimeSeries


cdef class Database:
    """
    A basic TempsDB object.

    :param path: path to the directory with the database
    """
    def __init__(self, path: str):
        self.path = path
        self.closed = False
        self.open_series = {}
        self.lock = threading.Lock()

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
                    return self.open_series[name]
                if not os.path.isdir(path):
                    raise DoesNotExist('series %s does not exist' % (name, ))
                self.open_series[name] = result = TimeSeries(path)
        return result

    def __del__(self):
        self.close()

    cpdef void close(self):
        """
        Close this TempsDB database
        """
        if self.closed:
            return
        cdef TimeSeries series
        for series in self.open_series.values():
            series.close()
        self.closed = True


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
