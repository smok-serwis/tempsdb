import ujson
from satella.files import read_in_file

from .chunks cimport Chunk
from .database cimport Database
from .exceptions import DoesNotExist, Corruption
import os


cdef class TimeSeries:
    def __init__(self, parent: Database, name: str):
        self.parent = parent
        self.name = name

        if not os.path.isdir(self.parent.path, name):
            raise DoesNotExist('Chosen time series does not exist')

        self.path = os.path.join(self.parent.path, self.name)


        cdef str metadata_s = read_in_file(os.path.join(self.path, 'metadata.txt'),
                                         'utf-8', 'invalid json')
        cdef dict metadata
        try:
            metadata = ujson.loads(metadata_s)
        except ValueError:
            raise Corruption('Corrupted series')

        cdef list files = os.path.listdir(self.path)
        cdef set files_s = set(files)
        files_s.remove('metadata.txt')
        self.chunks = []
        cdef str chunk
        for chunk in files_s:
            try:
                self.chunks.append(int(chunk))
            except ValueError:
                raise Corruption('Detected invalid file "%s"' % (chunk, ))

        self.last_entry_ts = metadata['last_entry_ts']
        self.block_size = metadata['block_size']

    cpdef void sync(self):
        """
        Synchronize the data kept in the memory with these kept on disk
        """
