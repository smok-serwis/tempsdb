import typing as tp
from .chunks cimport Chunk
from .series cimport TimeSeries
import collections


cdef class Iterator:
    """
    Iterator that allows iterating through result.

    At most basic this implements an iterator interface, iterating over
    tp.Tuple[int, bytes] - timestamp and data

    When you're done call :meth:`~tempsdb.iterators.Iterator.close` to release the resources.
    """
    def __init__(self, parent: TimeSeries, start: int, stop: int, chunks: tp.List[Chunk]):
        self.start = start
        self.stop = stop
        self.current_chunk = None
        self.chunks = collections.deque(chunks)
        self.parent = parent
        self.i = 0
        self.limit = 0
        self.closed = False
        self.is_first = False
        self.is_last = False

    def __del__(self):
        self.close()

    cpdef void close(self):
        """
        Close this iterator, release chunks.

        It is imperative that you call this, otherwise some chunks might remain in memory.

        This is hooked by destructor, but release it manually ASAP.

        No-op if iterator is already closed.
        """
        if self.closed:
            return
        self.closed = True
        cdef Chunk chunk
        for chunk in self.chunks:
            self.parent.decref_chunk(chunk.name())
        self.chunks = None

    cdef int get_next(self) except -1:
        """
        Fetch next chunk, set i, is_first, is_last and limit appropriately
        """
        if self.current_chunk is not None:
            self.parent.decref_chunk(self.current_chunk.name())
            self.is_first = False
        else:
            self.is_first = True

        try:
            self.current_chunk = self.chunks.popleft()
        except IndexError:
            raise StopIteration()

        self.is_last = not self.chunks

        if self.is_last and self.is_first:
            self.i = self.current_chunk.find_left(self.start)
            self.limit = self.current_chunk.find_right(self.stop)
        elif self.is_first:
            self.i = self.current_chunk.find_left(self.start)
            self.limit = self.current_chunk.length()
        elif self.is_last:
            self.i = 0
            self.limit = self.current_chunk.find_right(self.stop)
        else:
            self.i = 0
            self.limit = self.current_chunk.length()
        return 0

    def __next__(self) -> tp.Tuple[int, bytes]:
        cdef tuple tpl = self.next()
        if tpl is None:
            raise StopIteration()
        return tpl

    cpdef tuple next(self):
        """
        Return next element or None, if list was exhausted
        
        :return: next element
        :rtype: tp.Optional[tp.Tuple[int, bytes]]
        """
        try:
            if self.current_chunk is None:
                self.get_next()
            elif self.i == self.limit:
                self.get_next()
            return self.current_chunk.get_piece_at(self.i)
        except StopIteration:
            return None
        finally:
            self.i += 1

    def __iter__(self) -> Iterator:
        return self