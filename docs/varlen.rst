Variable length series
======================

.. versionadded:: 0.5

How does it work?
-----------------

They work by breaking down your data into smaller pieces and storing them in separate
series, prefixing with length.

For each series you specify so-called length profile. It is a list of ints, each representing
a block size for next series created. If an entry cannot fit in the already created series, a new one
will be created. Note that the last entry of this array will loop forever, so if you for example
put a 1024 byte data in a varlen series of length profile [10, 255] there will be a total
of 5 normal time series created to accommodate it, with length of:

* 10
* 255
* 255
* 255
* 255

Note that an entry is written to enough series so that it fits. For example, a 8 byte piece of data
would be written to only to the first series.

Each entry is also prefixed by it's length, so the actual size of the first
series is larger by that. The size of that field is described by an
extra parameter called `size_struct`. It represents an unsigned number.

Note that the only valid sizes of `size_struct` are:

* 1 for maximum length of 255
* 2 for maximum length of 65535
* 3 for maximum length of 16777215
* 4 for maximum length of 4294967295

Also note that variable length series live in a different namespace than standard
time series, so you can name them the same.

Accessing them
--------------

Use methods :meth:`tempsdb.database.Database.create_varlen_series` and
:meth:`tempsdb.database.Database.get_varlen_series` to obtain instances of following class:


.. autoclass:: tempsdb.varlen.VarlenSeries
    :members:


.. autoclass:: tempsdb.varlen.VarlenIterator
    :members:


.. autoclass:: tempsdb.varlen.VarlenEntry
    :members:

