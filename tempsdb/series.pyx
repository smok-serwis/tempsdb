from .database cimport Database

cdef class TimeSeries:
    def __init__(self, parent: Database, name: str):
        self.parent = parent
        self.name = name
