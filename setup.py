from setuptools import setup
from subprocess import run as running
import setuptools
import os

import pathlib

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

setup(name='PoolFlow',
      version='0.0.6',
      description='A simple process management library',
      url='https://victorgarric.github.io/PoolFlow/',
      license='MIT',
      long_description=README,
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
