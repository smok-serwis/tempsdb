# tempsdb

[![PyPI](https://img.shields.io/pypi/pyversions/tempsdb.svg)](https://pypi.python.org/pypi/tempsdb)
[![PyPI version](https://badge.fury.io/py/tempsdb.svg)](https://badge.fury.io/py/tempsdb)
[![PyPI](https://img.shields.io/pypi/implementation/tempsdb.svg)](https://pypi.python.org/pypi/tempsdb)
[![Documentation Status](https://readthedocs.org/projects/tempsdb/badge/?version=latest)](http://tempsdb.readthedocs.io/en/latest/?badge=latest)
[![Maintainability](https://api.codeclimate.com/v1/badges/657b03d115f6e001633c/maintainability)](https://codeclimate.com/github/smok-serwis/tempsdb/maintainability)
[![Build status](https://circleci.com/gh/smok-serwis/tempsdb.svg?style=shield)](https://app.circleci.com/pipelines/github/smok-serwis/tempsdb)

Embedded Cython library for time series that you need to upload somewhere.

Stored time series with a 8-bit timestamp and a fixed length of data.
So no variable encoding for you!

# Changelog

## v0.5

* _TBA_

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
