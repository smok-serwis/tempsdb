Variable length series
======================

Version 0.5 added support for variable length series.

How does it work?
-----------------

They work by breaking down your data into smaller pieces and storing them in separate
series, prefixing with length.

For each series you specify so-called length profile. It is a list of ints, each representing
a block size for next series created. If an entry cannot fit in the already created series, a new one
will be created. Note that the last entry of this array will loop forever, so if you for example
put a 1024 byte data in a varlen series of length profile [10, 255] there will be a total
of 5 normal time series created to accomodate it, with length of:
* 10
* 255
* 255
* 255
* 255

Each entry is also prefixed by it's length. The size of that field is described by an
extra parameter called `size_struct`.

Note that the only valid sizes of `size_struct` are:
* 1 for maximum length of 255
* 2 for maximum length of 65535
* 4 for maximum length of 4294967295

Accessing them
--------------

Use methods :meth:`tempsdb.database.Database.create_varlen_series` and
:meth:`tempsdb.database.Database.get_varlen_series` to obtain instances of following class:


.. autoclass:: tempsdb.varlen.VarlenSeries
    :members:


.. autoclass:: tempsdb.varlen.VarlenEntry
    :members:

