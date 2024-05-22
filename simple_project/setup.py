"""
Setup script for datakickstart_dabs.

This script packages and distributes the associated wheel file(s).
Source code is in ./src/. Run 'python setup.py sdist bdist_wheel' to build.
"""
from setuptools import setup, find_packages

import sys
sys.path.append('./src')

import datakickstart_dabs

setup(
    name="datakickstart_dabs",
    version=datakickstart_dabs.__version__,
    url="https://databricks.com",
    author="training@dustinvannoy.com",
    description="my test wheel",
    packages=find_packages(where='./src'),
    package_dir={'': 'src'},
    entry_points={"entry_points": "main=datakickstart_dabs.main:main"},
    install_requires=["setuptools"],
)
