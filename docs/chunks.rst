Chunk
=====

For your convenience the class :class:`~tempsdb.chunks.Chunk` was also documented, but don't use
it directly:

.. autoclass:: tempsdb.chunks.Chunk
    :members:

Data stored in files is little endian.


A file storing a chunk consists as follows:

* 4 bytes unsigned int - block size
* repeated
    * 8 bytes unsigned long long - timestamp
    * block_size bytes of data
