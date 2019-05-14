import sys
if sys.version_info.major < 3 or sys.version_info.minor < 2:
    raise ValueError("Dalmatian is only compatible with Python 3.3 and above")
if sys.version_info.minor < 5:
    import warnings
    warnings.warn("Dalmatian may not function properly on Python < 3.5")
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
    long_description_content_type = 'text/markdown',
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
        'firecloud>=0.16.9',
        'ipython',
        'iso8601',
        'google-cloud-storage>=1.13.2',
        'agutil>=4.0.2',
        'crayons>=0.2.0',
        'hound>=0.1.2',
    ],
    classifiers = [
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",
    ],
)
