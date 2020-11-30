import os
import typing as tp
from satella.files import find_files
from distutils.core import setup
from snakehouse import Multibuild, build


def find_pyx(*path) -> tp.List[str]:
    return list(find_files(os.path.join(*path), r'(.*)\.pyx', scan_subdirectories=True))


setup(name='tempsdb',
      version='0.1_a2',
      packages=['tempsdb'],
      install_requires=['satella>=2.14.21', 'ujson'],
      ext_modules=build([Multibuild('tempsdb', find_pyx('tempsdb')), ],
                        compiler_directives={
                            'language_level': '3',
                        }),
      python_requires='!=2.7.*,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*,!=3.5.*,!=3.6.*,!=3.7.*',
      test_suite="tests",
      zip_safe=False
      )
