cdef class Database:
    """
    A basic TempsDB object.
    """
    def __init__(self, path: str):
        self.path = path

