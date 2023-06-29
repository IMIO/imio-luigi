# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open("README.rst") as f:
    long_description = f.read()

with open("CHANGES.rst") as f:
    long_description += "\n" + f.read()

setup(
    name="imio-luigi",
    version="1.0.0",
    description="iMio Luigi ETL",
    long_description=long_description,
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
    ],
    author="iMio",
    author_email="support@imio.be",
    keywords="Python",
    url="https://github.com/imio/imio_luigi",
    license="GPL version 2",
    packages=find_packages(exclude=("docs")),
    install_requires=[
        "PyMySQL",
        "click",
        "jsonschema",
        "luigi",
        "setuptools",
        "sqlalchemy",
    ],
    extras_require={
        "test": [
            "nose",
        ],
        "docs": [
            "sphinx",
        ],
    },
    entry_points={
        'console_scripts': [
            "failure_report = imio_luigi.report.failure:main",
            "access_report = imio_luigi.report.access:main",
        ],
    }
)
