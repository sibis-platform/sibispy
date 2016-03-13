##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##
from setuptools import setup

setup(name='sibis',
      version='0.0.2',
      description='Scalable Informatics for Biomedical Imaging Studies',
      url='http://github.com/sibis-platform/sibis',
      author='Nolan Nichols',
      author_email='nolan.nichols@gmail.com',
      license='BSD',
      packages=['sibis'],
      package_data={"sibis": []},
      include_package_data=True,
      zip_safe=False,
      install_requires=['pyyaml', 'pyxnat', 'pycap', 'pygithub', 'requests'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest', 'pytest-capturelog', 'coverage'],
      scripts=['bin/sibis'])
