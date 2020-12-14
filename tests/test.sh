#!/bin/bash
python -m coverage run -m nose2 -vv
python -m coverage report
