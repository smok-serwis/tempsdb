# tempsdb

[![PyPI](https://img.shields.io/pypi/pyversions/tempsdb.svg)](https://pypi.python.org/pypi/tempsdb)
[![PyPI version](https://badge.fury.io/py/tempsdb.svg)](https://badge.fury.io/py/tempsdb)
[![PyPI](https://img.shields.io/pypi/implementation/tempsdb.svg)](https://pypi.python.org/pypi/tempsdb)
[![Documentation Status](https://readthedocs.org/projects/tempsdb/badge/?version=latest)](http://tempsdb.readthedocs.io/en/latest/?badge=latest)
[![Maintainability](https://api.codeclimate.com/v1/badges/657b03d115f6e001633c/maintainability)](https://codeclimate.com/github/smok-serwis/tempsdb/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/a0ff30771c71e43e8149/test_coverage)](https://codeclimate.com/github/smok-serwis/tempsdb/test_coverage)
[![Build Status](https://travis-ci.com/smok-serwis/tempsdb.svg)](https://travis-ci.com/smok-serwis/tempsdb)
[![Wheel](https://img.shields.io/pypi/wheel/tempsdb.svg)](https://pypi.org/project/tempsdb/)
[![License](https://img.shields.io/pypi/l/tempsdb)](https://github.com/smok-serwis/tempsdb)

Embedded Cython library for time series that you need to upload somewhere.

Stored time series with a 8-byte timestamp and a data, which can be of
fixed length or variable.

# Installation

```bash
git clone https://github.com/smok-serwis/tempsdb
cd tempsdb
pip install snakehouse tempsdb
python setup.py install
```

You need both [snakehouse](https://pypi.org/project/snakehouse/1.2.2/)
and [tempsdb](https://pypi.org/project/tempsdb/) to compile it from the source,
though binary wheels for:
 
* Linux [manylinux2014]:
    * x86_64:
        * Python 3.8
        * Python 3.9
    * armv7l:
        * Python 3.8


are available. If you need an extra wheel, compile it yourself
or just drop me an [issue](https://github.com/smok-serwis/tempsdb/issues/new).

If you're installing it somewhere that you don't need both snakehouse
and tempsdb installed, compile your own binary wheel with

```
python setup.py bdist_wheel
```

Then copy your resulting wheel and install it via pip on the target system.

Be aware that tempsdb does logging. 
Consult the docs for how to disable it.

# Changelog

## v0.6.5

* fixed warnings about signed to unsigned comparison

## v0.6.4

* fixed a bug with slicing chunks in `VarlenSeries`
* added extra comparison operators for `VarlenEntry`
* added `sync` to `VarlenSeries`
* fixed a bug with not propagating metadata write exceptions
* fixed a bug with `Database` treating `varlen` and metadata as real time series


## v0.6.3

* added logging for opening and closing series

## v0.6.2

* added the context manager syntax to VarlenIterator
* fixed a memory leak that happened during getting current value
    from an empty series
* added `MemoryPressureManager` support for `Database`

## v0.6.1

* fixed an issue with `Iterators`

## v0.6

* **bugfix**: fixed some bugs with reading values after close
* added support for storing metadata as minijson
    * this will be enabled by default is minijson is importable
* fixed minor compiler warnings
* `TimeSeries.iterate_range` will accept a parameter called
  `direct_bytes` for compatibility with `VarlenSeries`.
  It's value is ignored
* more class constructors use explicit typing - faster tempsdb
* `TimeSeries.get_current_value` will correctly raise `ValueError` instead of returning None

## v0.5.4

* older TempsDB databases that do not support varlens will be updated upon opening
* added metadata support for databases
* a flush will be done before re-enabling mmap
* bugfix to read archive data

## v0.5.3

* added `disable_mmap`, `enable_mmap` and `open_chunks_mmap_size` into `VarlenSeries`

## v0.5.2

* added multiple properties and attributes to `VarlenSeries`

## v0.5.1

* added `VarlenSeries.close_chunks`
* `Database.sync` will now return 0
* indexed-gzip proved to be a poor choice, dropped
* `setup.py` fixed

## v0.5

* if mmap is used, the kernel will be informed after loading the chunk that we 
  don't need it's memory right now
* deleting a `TimeSeries` will now correctly return a zero
* both `Database`, `TimeSeries` and `Chunk` destructor will close and 
  emit a warning if the user forgot to
* if page_size is default, it won't be written as part of the metadata
* added support for per-series metadata
* following additions to `Database`:
    * `delete_series`
    * `delete_varlen_series`
* following additions to `TimeSeries`:
    * added `append_padded`
    * added metadata support, `metadata` property and `set_metadata` call
* added variable length series
* added experimental support for gzipping time series
* fixed a bug where getting a series that was already closed would TypeError
* following additions to `Chunk`:
    * `get_slice_of_piece_at`
    * `get_slice_of_piece_starting_at`
    * `get_byte_of_piece`
    * `get_timestamp_at`
* fixed the behaviour of `AlternativeMMaps` when passed a single index to __getitem__ and __setitem__
* added `StillOpen` exception, chunk won't allow to close itself if it has any
  remaining references

## v0.4.4

* more error conditions during mmap will be supported as well
* ENOMEM will be correctly handled during resize operation
* added `TimeSeries.descriptor_based_access`
* added `Chunk.switch_to_mmap_based_access`

## v0.4.3

* improving handling mmap failures on too low memory
* slightly reduced `metadata.txt` by defaulting `page_size`
* moved `Chunk`
* added support for gzipping
* added `DirectChunk`
* iterating and writing at the same time from multiple threads
    made safe
* added `TimeSeries.disable_mmap`
* `Iterator`'s destructor will emit a warning if you forget to close it explicitly.
* added option for transparent gzip compression
    Please note that gzip disables mmap!
* experimental gzip support for constant-length time series

## v0.4.2

* empty series will return an Iterator
* **bugfix release** fixed `Database.create_series`
* `Database` constructor will throw if no database is there
* changed `Iterator.next` to `Iterator.next_item`, 
  synce Cython guys said to not implement the method `next`
  on iterators.

## v0.4.1

* **bugfix release** fixed `get_open_series`

## v0.4

* can install from sdist now

## v0.3

* added `TimeSeries.get_current_value`
* added `Database.sync`

## v0.2

* added `get_open_series`
* added `get_all_series`
* added `get_first_entry_for`
* added `close_all_open_series`
* added `TimeSeries.name`
* added option to use descriptor based access instead of mmap
* added `TimeSeries.open_chunks_ram_size`

## v0.1

First release
