import codecs
import os
import re

from setuptools import setup, find_packages


def find_version(*file_paths):
    """
    Don't pull version by importing package as it will be broken due to as-yet uninstalled
    dependencies, following recommendations at  https://packaging.python.org/single_source_version,
    extract directly from the init file
    """
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, *file_paths), 'r', encoding="utf-8") as f:
        version_file = f.read()

    # The version line must have the form
    # __version__ = 'ver'
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(

    packages=find_packages(exclude=["tests*"]),

    # Not all packages are capable of running in compressed form,
    # because they may expect to be able to access either source
    # code or data files as normal operating system files.
    zip_safe=False,

    install_requires=[
        "tables",
        "pandas"
    ],

    name="tableseries",
    version=find_version("tableseries", "__init__.py"),

    # metadata for upload to PyPI
    author="Winton Wang",
    author_email="365504029@qq.com",
    description="Handles large time series using HDF5 and Pandas",
    license="LGPLv3",
    keywords="time series,HDF5",
    url="http://github.com/nooperpudd/tableseries",  # project home page, if any
    long_description=open('README.md').read(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries"
    ]
)
