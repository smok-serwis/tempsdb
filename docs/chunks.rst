Chunk
=====

.. versionadded:: 0.5

There are two kinds of chunk - a "normal" chunk and "direct" chunk.

The difference is that a normal chunk will preallocate a page ahead, in order for writes to be fast,
while direct chunk will write only as much data as is strictly required.

Only "direct" chunks are capable to be gzipped, since one page is preallocated for normal chunk, which
would prevent modifications made post-factum to it.

For your convenience the class :class:`~tempsdb.chunks.base.Chunk` was also documented, but don't use
it directly:

.. autoclass:: tempsdb.chunks.base.Chunk
    :members:

Data stored in files is little endian.

A way to tell which chunk are we dealing with is to look at it's extension.
Chunks that have:

* no extension - are normal chunks
* `.direct` extension - are direct chunks
* `.gz` extension - are direct and gzipped chunks

Normal chunk
------------
A file storing a normal chunk consists as follows:

* 4 bytes unsigned int - block size
* repeated
    * 8 bytes unsigned long long - timestamp
    * block_size bytes of data

It's padded to `page_size` with zeros, and four last bytes is the `unsigned long` amount of entries

Direct chunk
------------
A file storing a direct chunk consists as follows:

* 4 bytes unsigned int - block size
* repeated
    * 8 bytes unsigned long long - timestamp
    * block_size bytes of data

Note that a direct chunk will be able to be gzipped. If it's file name ends with .gz, then it's
a direct chunk which is gzipped.
