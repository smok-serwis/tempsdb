import os
import shutil
import threading
import warnings

from satella.coding import DictDeleter

from tempsdb.exceptions import DoesNotExist, AlreadyExists, StillOpen
from .series cimport TimeSeries, create_series
from .varlen cimport VarlenSeries
from .metadata cimport read_meta_at, write_meta_at

cdef class Database:
    """
    A basic TempsDB object.

    After you're done with it, please call
    :meth:`~tempsdb.database.Database.close`.

    If you forget to, the destructor will do that instead and emit a warning.

    :param path: path to the directory with the database
    :raises DoesNotExist: database does not exist, use `create_database`

    :ivar path: path to  the directory with the database (str)
    :ivar metadata: metadata of this DB
    """
    def __init__(self, str path):
        if not os.path.isdir(path):
            raise DoesNotExist('Database does not exist')

        if not os.path.isdir(os.path.join(path, 'varlen')):
            os.mkdir(os.path.join(path, 'varlen'))

        self.path = path
        self.closed = False
        self.open_series = {}
        self.open_varlen_series = {}
        self.lock = threading.RLock()
        self.mpm = None
        self.mpm_handler = None
        self.metadata = {}
        self.reload_metadata()

    cpdef int reload_metadata(self) except -1:
        """
        Try to load the metadata again.
        
        This will change `metadata` attribute.
        """
        self.metadata = read_meta_at(self.path)
        return 0

    cpdef int set_metadata(self, dict metadata) except -1:
        """
        Set metadata for this series.
        
        This will change `metadata` attribute.
        
        :param metadata: new metadata to set
        """
        write_meta_at(self.path, metadata)
        self.metadata = metadata
        return 0

    cpdef list get_open_series(self):
        """
        Return all open series
        
        :return: open series
        :rtype: tp.List[tp.Union[VarlenSeries, TimeSeries]]
        """
        cdef:
            list output = []
            TimeSeries series
            VarlenSeries v_series
            str name
        with self.lock:
            with DictDeleter(self.open_series) as dd:
                for series in dd.values():
                    if series.closed:
                        dd.delete()
                    else:
                        output.append(series)
            with DictDeleter(self.open_varlen_series) as dd:
                for v_series in dd.values():
                    if v_series.closed:
                        dd.delete()
                    else:
                        output.append(v_series)
        return output

    cpdef int checkpoint(self) except -1:
        """
        Destroy closed series
        """
        cdef:
            TimeSeries series
            VarlenSeries v_series
        with self.lock:
            with DictDeleter(self.open_series) as dd:
                for series in dd.values():
                    if series.closed:
                        dd.delete()
            with DictDeleter(self.open_varlen_series) as dd:
                for v_series in dd.values():
                    if v_series.closed:
                        dd.delete()
        return 0

    cpdef int delete_series(self, str name) except -1:
        """
        Deletes a constant-length time series.
        
        Note that the series must either not be open, or closed.
        
        :param name: name of series to delete
        :raises ValueError: tried to delete "varlen" series
        :raises StillOpen: series is open
        """
        if name == 'varlen':
            raise ValueError('tried to delete varlen series')
        if not os.path.exists(os.path.join(self.path, name)):
            raise DoesNotExist('series does not exist')
        cdef TimeSeries series
        with self.lock:
            if name in self.open_series:
                series = self.open_series[name]
                if not series.closed:
                    raise StillOpen('series is open!')
            shutil.rmtree(os.path.join(self.path, name))
            return 0

    cpdef int delete_varlen_series(self, str name) except -1:
        """
        Deletes a variable-length time series.
        
        Note that the series must either not be open, or closed.
        
        :param name: name of series to delete
        :raises DoesNotExist: series does not exist
        :raises StillOpen: series is open
        """
        cdef:
            str path = os.path.join(self.path, 'varlen', name)
            VarlenSeries series
        if not os.path.exists(path):
            raise DoesNotExist('series does not exist')
        with self.lock:
            if name in self.open_varlen_series:
                series = self.open_varlen_series[name]
                if not series.closed:
                    raise StillOpen('series is open!')
            shutil.rmtree(path)
            return 0

    cpdef TimeSeries get_series(self, str name, bint use_descriptor_based_access = False):
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
        with self.lock:
            if name in self.open_series:
                result = self.open_series[name]
                if result.closed:
                    del self.open_series[name]
                    return self.get_series(name)
            else:
                path = os.path.join(self.path, name)
                if not os.path.isdir(path):
                    raise DoesNotExist('series %s does not exist' % (name, ))
                self.open_series[name] = result = TimeSeries(path, name,
                                                             use_descriptor_based_access=use_descriptor_based_access)
                if self.mpm is not None:
                    result.register_memory_pressure_manager(self.mpm)
        return result

    cpdef int close_all_open_series(self) except -1:
        """
        Closes all open series.
        
        Note that this won't close variable length series that are in-use.
        """
        cdef:
            TimeSeries series
            VarlenSeries v_series
        with self.lock:
            for series in self.open_series.values():
                series.close()
            self.open_series = {}
            with DictDeleter(self.open_varlen_series) as dd:
                for v_series in dd.values():
                    try:
                        v_series.close()
                        dd.delete()
                    except StillOpen:
                        pass
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
        return 0

    cpdef list get_all_normal_series(self):
        """
        Stream all constant-length series available within this database
                
        :return: a list of series names
        :rtype: tp.List[str]
        """
        return os.listdir(self.path)

    cpdef list get_all_varlen_series(self):
        """
        Stream all variable-length series available within this database
                
        :return: a list of series names
        :rtype: tp.List[str]
        """
        return os.listdir(os.path.join(self.path, 'varlen'))

    cpdef VarlenSeries create_varlen_series(self, str name, list length_profile,
                                            int size_struct,
                                            unsigned long entries_per_chunk,
                                            int gzip_level=0):
        """
        Create a new variable length series
        
        :param name: name of the series
        :param length_profile: list of lengths of subsequent chunks
        :param size_struct: how many bytes will be used to store length?
            Valid entries are 1, 2 and 4
        :param entries_per_chunk: entries per chunk file
        :param gzip_level: level of gzip compression. Leave at 0 (default) to disable compression.
        :return: new variable length series
        :raises AlreadyExists: series with given name already exists
        """
        from .varlen import create_varlen_series

        cdef:
            VarlenSeries series
            str path = os.path.join(self.path, 'varlen', name)
        with self.lock:
            if os.path.isdir(path):
                raise AlreadyExists('Series already exists')
            series = create_varlen_series(path, name,
                                          size_struct,
                                          length_profile,
                                          entries_per_chunk,
                                          gzip_level=gzip_level)
            self.open_varlen_series[name] = series
        return series


    cpdef VarlenSeries get_varlen_series(self, str name):
        """
        Load and return an existing variable length series
        
        :param name: name of the series
        
        :return: a loaded varlen series
        :raises DoesNotExist: series does not exist
        """
        cdef:
            VarlenSeries result
            str path
        with self.lock:
            if name in self.open_varlen_series:
                result = self.open_varlen_series[name]
                if result.closed:
                    del self.open_varlen_series[name]
                    result = self.get_varlen_series(name)
            else:
                path = os.path.join(self.path, 'varlen', name)
                if not os.path.isdir(path):
                    raise DoesNotExist('series %s does not exist' % (name, ))
                self.open_varlen_series[name] = result = VarlenSeries(path, name)
                if self.mpm is not None:
                    result.register_memory_pressure_manager(self.mpm)
        return result

    cpdef TimeSeries create_series(self, str name, int block_size,
                                   unsigned long entries_per_chunk,
                                   int page_size=0,
                                   bint use_descriptor_based_access=False,
                                   int gzip_level=0):
        """
        Create a new series.
        
        Note that series cannot be named "varlen" or "metadata.txt" or "metadata.minijson"
        
        :param name: name of the series
        :param block_size: size of the data field
        :param entries_per_chunk: entries per chunk file
        :param page_size: size of a single page. Default (0) is autodetect.
        :param use_descriptor_based_access: whether to use descriptor based access instead of mmap.
            Default is False
        :param gzip_level: gzip compression level. Default is 0 which means "don't use gzip"
        :return: new series
        :raises ValueError: block size was larger than page_size plus a timestamp or series was named 
            "varlen"
        :raises AlreadyExists: series with given name already exists
        """
        if block_size > page_size + 8:
            raise ValueError('Invalid block size, pick larger page')
        if name == 'varlen' or name == 'metadata.txt' or name == 'metadata.minijson':
            raise ValueError('Series cannot be named varlen or metadata.txt or metadata.minijson')
        if os.path.isdir(os.path.join(self.path, name)):
            raise AlreadyExists('Series already exists')
        cdef TimeSeries series
        with self.lock:
            series = create_series(os.path.join(self.path, name), name,
                                   block_size,
                                   entries_per_chunk, page_size=page_size,
                                   use_descriptor_based_access=use_descriptor_based_access,
                                   gzip_level=gzip_level)
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
        self.mpm_handler = mpm.register_on_entered_severity(1)(self.checkpoint)
        return 0

    def __del__(self):
        if not self.closed:
            warnings.warn('You forgot the close the Database. Please close it explicitly when you '
                          'are done.', )
            self.close()

    cpdef int close(self) except -1:
        """
        Close this TempsDB database
        """
        if self.closed:
            return 0
        cdef:
            TimeSeries series
            VarlenSeries var_series
        with self.lock:
            for series in self.open_series.values():
                series.close()  # because already closed series won't close themselves
            self.open_series = {}
            for var_series in self.open_varlen_series.values():
                var_series.close(True)
            self.open_varlen_series = {}
        self.closed = True
        if self.mpm_handler is not None:
            self.mpm_handler.cancel()
            self.mpm_handler = None
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
    os.mkdir(os.path.join(path, 'varlen'))
    return Database(path)
