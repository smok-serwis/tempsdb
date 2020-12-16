import os
import typing as tp

from Cython.Build import cythonize
from satella.files import find_files
from distutils.core import setup

from setuptools import find_packages, Extension
from snakehouse import Multibuild, build, monkey_patch_parallel_compilation


def find_pyx(*path) -> tp.List[str]:
    return list(find_files(os.path.join(*path), r'(.*)\.pyx', scan_subdirectories=True))


monkey_patch_parallel_compilation()

directives = {'language_level': '3'}
ext_kwargs = {}
cythonize_kwargs = {}
if 'CI' in os.environ:
    ext_kwargs['define_macros'] = [("CYTHON_TRACE_NOGIL", "1")]
    directives.update(profile=True, linetrace=True, embedsignature=True)
    cythonize_kwargs['gdb_debug'] = True
    # extensions = [
    #     Extension('tempsdb.database', ['tempsdb/database.pyx']),
    #     Extension('tempsdb.database', ['tempsdb/database.pyx']),
    #     Extension('tempsdb.exceptions', ['tempsdb/exceptions.pyx']),
    #     Extension('tempsdb.iterators', ['tempsdb/iterators.pyx']),
    #     Extension('tempsdb.series', ['tempsdb/series.pyx']),
    #     Extension('tempsdb.varlen', ['tempsdb/varlen.pyx']),
    #     Extension('tempsdb.chunks.gzip', ['tempsdb/chunks/gzip.pyx']),
    #     Extension('tempsdb.chunks.direct', ['tempsdb/chunks/direct.pyx']),
    #     Extension('tempsdb.chunks.normal', ['tempsdb/chunks/normal.pyx']),
    #     Extension('tempsdb.chunks.maker', ['tempsdb/chunks/maker.pyx']),
    #     Extension('tempsdb.chunks.base', ['tempsdb/chunks/base.pyx']),
    # ]
    # ext_modules = cythonize(extensions, compiler_directives=directives)
ext_modules = build([Multibuild('tempsdb', find_pyx('tempsdb'), **ext_kwargs), ],
                     compiler_directives=directives,
                     **cythonize_kwargs)

setup(name='tempsdb',
      version='0.5.1',
      packages=find_packages(include=['tempsdb', 'tempsdb.*']),
      install_requires=['satella>=2.14.24', 'ujson'],
      ext_modules=ext_modules,
      python_requires='!=2.7.*,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*,!=3.5.*,!=3.6.*,!=3.7.*',
      test_suite="tests",
      zip_safe=False
      )
