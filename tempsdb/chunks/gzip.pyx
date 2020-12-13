import gzip
import threading


cdef class ReadWriteGzipFile:
    def __init__(self, path: str, compresslevel: int = gzip._COMPRESS_LEVEL_FAST):
        self.path = path
        self.compress_level = compresslevel
        self.ro_file = gzip.GzipFile(path, 'rb', compresslevel=self.compress_level)
        self.rw_file = gzip.GzipFile(path, 'ab', compresslevel=self.compress_level)
        self.pointer = 0
        self.lock = threading.RLock()

    def flush(self):
        self.rw_file.flush()
        self.reopen_read()

    def size(self):
        cdef:
            bytes b
        with self.lock:
            self.seek(0, 0)
            b = self.read(128)
            while b:
                b = self.read(128)
        return self.pointer

    def close(self):
        with self.lock:
            self.ro_file.close()
            self.rw_file.close()

    cdef int reopen_read(self) except -1:
        with self.lock:
            self.ro_file.close()
            self.ro_file = gzip.GzipFile(self.path, 'rb', compresslevel=self.compress_level)
            self.pointer = 0
        return 0

    def read(self, int maxinplen):
        cdef bytes b
        with self.lock:
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
            self.reopen_read()

    def seek(self, unsigned long pos, int mode):
        if mode == 2:
            self.pointer = self.size()-pos
            self.ro_file.seek(self.pointer, 0)
        else:
            if pos != self.pointer:
                self.ro_file.seek(pos, mode)
                if mode == 0:
                    self.pointer = pos

    def tell(self):
        return self.pointer
