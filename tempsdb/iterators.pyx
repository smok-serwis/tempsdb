import typing as tp
import warnings

from .chunks.base cimport Chunk
from .series cimport TimeSeries
import collections


cdef class Iterator:
    """
    Iterator that allows iterating through result.

    Can be used as a context manager:

    >>> with series.iterate_range(0, 5000) as it:
    >>>     for timestamp, value in it:
    >>>         ...

    It will close itself automatically via destructor, if you forget to call close.

    At most basic this implements an iterator interface, iterating over
    tp.Tuple[int, bytes] - timestamp and data

    When you're done call :meth:`~tempsdb.iterators.Iterator.close` to release the resources.

    A warning will be emitted in the case that destructor has to call
    :meth:`~tempsdb.iterators.Iterator.close`.
    """

    def __init__(self, TimeSeries parent, unsigned long long start, unsigned long long stop, list chunks):
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        if not self.closed:
            warnings.warn('You forgot to close an Iterator. Please close them explicitly!')
            self.close()

    cpdef int close(self) except -1:
        """
        Close this iterator, release chunks.

        It is imperative that you call this, otherwise some chunks might remain in memory.

        This is hooked by destructor, but release it manually ASAP.

        No-op if iterator is already closed.
        """
        if self.closed:
            return 0
        self.closed = True
        cdef Chunk chunk
        for chunk in self.chunks:
            chunk.decref()
        self.chunks = None
        return 0

    cdef int get_next(self) except -1:
        """
        Fetch next chunk, set i, is_first, is_last and limit appropriately.
        
        Primes the iterator to do meaningful work.
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
        tpl = self.next_item()
        if tpl is None:
            raise StopIteration()
        return tpl

    cdef tuple next_item_pos(self):
        """
        Note that this increases the chunk reference count.
        
        :return: the timestamp of next element and a position of it within the current chunk,
            along with that chunk
        :rtype: tp.Tuple[int, int, Chunk]
        """
        try:
            if self.current_chunk is None:
                self.get_next()
            elif self.i == self.limit:
                self.get_next()
            self.current_chunk.incref()
            return self.current_chunk.get_timestamp_at(self.i), self.i, self.current_chunk
        except StopIteration:
            return None
        finally:
            self.i += 1

    cpdef tuple next_item(self):
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
        except (StopIteration, IndexError):
            return None
        finally:
            self.i += 1

    def __iter__(self) -> Iterator:
        return self
