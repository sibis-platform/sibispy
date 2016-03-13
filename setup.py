from setuptools import setup

setup(name='sibis',
      version='0.0.1',
      description='Scalable Informatics for Biomedical Imaging Studies',
      url='http://github.com/sibis-platform/sibis',
      author='Nolan Nichols',
      author_email='nolan.nichols@gmail.com',
      license='BSD',
      packages=['sibis'],
      include_package_data=True,
      zip_safe=False,
      setup_requires=['pytest-runner'],
      tests_require=['pytest', 'pytest-capturelog', 'coverage'],
      scripts=['bin/sibis'])
