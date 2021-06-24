#!/bin/bash
set -e
python -m coverage run -m nose2 -vv -F
python -m coverage report
