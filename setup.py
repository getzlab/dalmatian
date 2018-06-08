import os
import re
from setuptools import setup, find_packages
with open("dalmatian/__init__.py") as reader:
    __version__ = re.search(
        r'__version__ ?= ?[\'\"]([\w.]+)[\'\"]',
        reader.read()
    ).group(1)
_README           = os.path.join(os.path.dirname(__file__), 'README.md')
_LONG_DESCRIPTION = open(_README).read()

# Setup information
setup(
    name = 'firecloud-dalmatian',
    version = __version__,
    packages = find_packages(),
    description = 'A friendly companion for FISS',
    author = 'Broad Institute - Cancer Genome Computational Analysis',
    author_email = 'gdac@broadinstitute.org',
    long_description = _LONG_DESCRIPTION,
    entry_points = {
        'console_scripts': [
            'dalmatian = dalmatian.core:main'
        ]
    },
    install_requires = [
    'numpy',
    'matplotlib',
    'pandas',
    'pytz',
    'firecloud',
    'ipython',
    'iso8601'
    ],
    classifiers = [
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",
    ],
)
