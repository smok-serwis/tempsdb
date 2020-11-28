import os
import typing as tp
from satella.files import find_files
from setuptools import find_packages
from distutils.core import setup
from snakehouse import Multibuild, build


def find_pyx(*path) -> tp.List[str]:
    return list(find_files(os.path.join(*path), r'(.*)\.pyx', scan_subdirectories=True))


setup(name='tempsdb',
      version='0.1_a1',
      packages=find_packages(include=['tempsdb', 'tempsdb.*']),
      install_requires=['satella'],
      ext_modules=build([Multibuild('tempsdb', find_pyx('rapid')), ],
                        compiler_directives={
                            'language_level': '3',
                        }),
      python_requires='!=2.7.*,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*,!=3.5.*,!=3.6.*,!=3.7.*',
      zip_safe=False
      )
