import os
import threading

from tempsdb.exceptions import DoesNotExist
from .series cimport TimeSeries


cdef class Database:
    """
    A basic TempsDB object.
    """
    def __init__(self, path: str):
        self.path = path
        self.closed = False
        self.open_series = {}
        self.lock = threading.Lock()

    cpdef TimeSeries get_series(self, name: str):
        cdef TimeSeries result
        if name in self.open_series:
            result = self.open_series[name]
        else:
            with self.lock:
                if not os.path.isdir(os.path.join(self.path, name)):
                    raise DoesNotExist('series %s does not exist' % (name, ))
                self.open_series[name] = result = TimeSeries(self, name)
        return result


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
