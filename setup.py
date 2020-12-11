import os
import typing as tp

from Cython.Build import cythonize
from satella.files import find_files
from distutils.core import setup

from setuptools import Extension
from snakehouse import Multibuild, build
from satella.distutils import monkey_patch_parallel_compilation


def find_pyx(*path) -> tp.List[str]:
    return list(find_files(os.path.join(*path), r'(.*)\.pyx', scan_subdirectories=True))


monkey_patch_parallel_compilation()

# extensions = [Extension("tempsdb.chunks", ['tempsdb/chunks.pyx']),
#               Extension("tempsdb.database", ['tempsdb/database.pyx']),
#               Extension('tempsdb.exceptions', ['tempsdb/exceptions.pyx']),
#               Extension('tempsdb.series', ['tempsdb/series.pyx']),
#               Extension('tempsdb.iterators', ['tempsdb/iterators.pyx'])]
#
directives = {'language_level': '3'}
if 'CI' in os.environ:
    directives.update(profile=True, linetrace=True, embedsignature=True)


setup(name='tempsdb',
      version='0.5a2',
      packages=['tempsdb'],
      install_requires=['satella>=2.14.24', 'ujson'],
      ext_modules=build([Multibuild('tempsdb', find_pyx('tempsdb')), ],
                        compiler_directives=directives),
      # ext_modules=cythonize(extensions,
      #                   gdb_debug=True,
      #                   compiler_directives={
      #                       'language_level': '3',
      #                   }),
      python_requires='!=2.7.*,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*,!=3.5.*,!=3.6.*,!=3.7.*',
      test_suite="tests",
      zip_safe=False
      )
