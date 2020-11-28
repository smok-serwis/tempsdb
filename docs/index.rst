.. tempsdb documentation master file, created by
   sphinx-quickstart on Sat Nov 28 15:49:03 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to tempsdb's documentation!
===================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   time-series
   exceptions

It tries to use mmap where possible, and in general be as zero-copy as possible (ie. the
only time data is unserialized is when a particular entry is read).


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
