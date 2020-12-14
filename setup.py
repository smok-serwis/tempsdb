import os
import typing as tp

from satella.files import find_files
from distutils.core import setup

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


setup(name='tempsdb',
      version='0.5',
      packages=['tempsdb'],
      install_requires=['satella>=2.14.24', 'ujson', 'indexed_gzip'],
      ext_modules=build([Multibuild('tempsdb', find_pyx('tempsdb'), **ext_kwargs), ],
                        compiler_directives=directives,
                        **cythonize_kwargs),
      python_requires='!=2.7.*,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*,!=3.5.*,!=3.6.*,!=3.7.*',
      test_suite="tests",
      zip_safe=False
      )
