import os
import typing as tp

from satella.files import find_files
from distutils.core import setup

from setuptools import find_packages
from snakehouse import Multibuild, build, monkey_patch_parallel_compilation


def find_pyx(*path) -> tp.List[str]:
    return list(find_files(os.path.join(*path), r'(.*)\.pyx', scan_subdirectories=True))


monkey_patch_parallel_compilation()

directives = {'language_level': '3'}
ext_kwargs = {}
cythonize_kwargs = {}
if 'CI' in os.environ and 'RELEASE' not in os.environ:
    ext_kwargs['define_macros'] = [("CYTHON_TRACE_NOGIL", "1")]
    directives.update(profile=True, linetrace=True, embedsignature=True)
    cythonize_kwargs['gdb_debug'] = True

ext_modules = build([Multibuild('tempsdb', find_pyx('tempsdb'),
                                **ext_kwargs), ],
                     compiler_directives=directives,
                     **cythonize_kwargs)

setup(packages=find_packages(include=['tempsdb', 'tempsdb.*']),
      ext_modules=ext_modules,
      test_suite="tests",
      )
