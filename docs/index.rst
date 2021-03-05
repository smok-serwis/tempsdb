Welcome to tempsdb's documentation!
===================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   usage
   exceptions
   chunks
   varlen
   memory-pressure-manager

This is an append-only embedded time series library written in Cython.

It tries to use mmap for reads and writes, and in general is as zero-copy as possible (ie. the
only time data is unserialized is when a particular entry is read). It also uses
iterators.

Visit the project GitHub_ page!

.. _GitHub: https://github.com/smok-serwis/tempsdb

Stored time series with a 8-bit timestamp and a fixed length of data.
So no variable encoding for you!

.. versionadded:: 0.2

When mmap fails due to memory issues, this falls back to slower fwrite()/fread() implementation.
You can also manually select the descriptor-based implementation if you want to.

.. versionadded:: 0.5

Experimental support for gzipping time series is added. Note that reading from gzipped files might be
slow, as every seek requires reading the file from the beginning.

Warnings will be issued while using gzipped series to remind you of this fact.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
