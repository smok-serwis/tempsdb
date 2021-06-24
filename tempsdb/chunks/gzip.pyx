import gzip
import threading


cdef class ReadWriteGzipFile:
    def __init__(self, str path, int compresslevel = gzip._COMPRESS_LEVEL_FAST):
        self.path = path
        self.compress_level = compresslevel
        self.rw_file = gzip.GzipFile(path, 'ab', compresslevel=self.compress_level)
        self.ro_file = gzip.GzipFile(path, 'rb')
        self.pointer = 0
        self.lock = threading.RLock()
        self.needs_flush_before_read = False
        cdef bytes b
        b = self.read(128)
        while b:
            b = self.read(128)
        self.size = self.pointer

    cpdef int flush(self) except -1:
        self.ro_file.close()
        self.rw_file.flush()
        self.ro_file = gzip.GzipFile(self.path, 'rb')
        self.pointer = 0
        self.needs_flush_before_read = False
        return 0

    def close(self):
        with self.lock:
            self.rw_file.close()
            self.ro_file.close()

    def read(self, int maxinplen):
        cdef bytes b
        with self.lock:
            if self.needs_flush_before_read:
                self.flush()
            b = self.ro_file.read(maxinplen)
            self.pointer += len(b)
        return b

    def write(self, bytes value):
        """
        Always an append, despite what
        :meth:`~tempsdb.chunks.gzip.ReadWriteGzipFile.tell` and
        :meth:`~tempsdb.chunks.gzip.ReadWriteGzipFile.seek` may say.
        """
        with self.lock:
            self.rw_file.write(value)
            self.size += len(value)
            self.needs_flush_before_read = True

    def seek(self, unsigned long pos, int mode):
        if self.needs_flush_before_read:
            self.flush()
        if mode == 2:
            self.seek(self.size-pos, 0)
        elif mode == 0:
            if pos != self.pointer:
                self.ro_file.seek(pos, 0)
                self.pointer = pos
        elif mode == 1:
            raise NotImplementedError('Unimplemented seek mode')
        else:
            raise ValueError('Invalid seek mode')

    def tell(self):
        return self.pointer
