# -*- coding: utf-8 -*-
"""
    Setup file for Pandas-ETL.
    Use setup.cfg to configure your project.
"""
import sys

from pkg_resources import VersionConflict, require
from setuptools import setup

try:
    require("setuptools>=38.3")
except VersionConflict:
    print("Error: version of setuptools is too old (<38.3)!")
    sys.exit(1)


if __name__ == "__main__":
    setup(name='pyetl',
      version='0.0.1',
      description='ETL Utilities using Python Pandas',
      author='Neel Puniwala',
      author_email='neelpuniwala1996@gmail.com')
