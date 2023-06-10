from setuptools import setup
from subprocess import run as running
import setuptools
import os

setup(name='PoolFlow',
      version='0.0.1',
      description='Job flow management',
      url='https://victorgarric.github.io/PoolFlow/',
      license='MIT',
      long_description='README',
      long_description_content_type="text/markdown",
      author='Victor Garric',
      packages=setuptools.find_packages(),
      author_email='victor.garric@gmail.com',
      include_package_data=True,
      keywords='',
      install_requires=['rich'],
      entry_points={"console_scripts": ["", ], },
      tests_require=['pytest'],
      zip_safe=False)
