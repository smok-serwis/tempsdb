cdef class Database:
    """
    A basic TempsDB object.
    """
    def __init__(self, path: str):
        self.path = path
        self.closed = False

    cpdef void close(self):
        """
        Close this TempsDB database
        """
        if self.closed:
            return
        self.closed = True
