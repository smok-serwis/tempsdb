How this does work?
===================

.. note:: This is about fixed length data time series.

Data is stored in so called chunks. A chunk's last page can be actively appended to, or a chunk
is immutable.

When there is a request to fetch some data, a chunk is loaded into memory. It will not
be automatically unloaded, to do this, you must periodically call
:meth:`~tempsdb.series.TimeSeries.close_chunks`.

Usage
=====

Start off by instantiating an object

.. autoclass:: tempsdb.database.Database
    :members:

You can create new databases via

.. autofunction:: tempsdb.database.create_database

Then you can create and retrieve particular series:

.. autoclass:: tempsdb.series.TimeSeries
    :members:

You retrieve their data via Iterators:

.. autoclass:: tempsdb.iterators.Iterator
    :members:

Appending the data is done via :meth:`~tempsdb.series.TimeSeries.append`. Since time series are
allocated in entire pages, so your files will be padded to a page in size. This makes writes
quite fast, as in 99.9% cases it is just a memory operation.

