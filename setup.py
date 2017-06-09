import os
from setuptools import setup, find_packages
from dalmatian.__about__ import __version__
_README           = os.path.join(os.path.dirname(__file__), 'README')
_LONG_DESCRIPTION = open(_README).read()

# Setup information
setup(
    name = 'dalmatian',
    version = __version__,
    packages = find_packages(),
    description = 'A friendly companion for FISS',
    author = 'Broad Institute - Cancer Genome Computational Analysis',
    author_email = 'gdac@broadinstitute.org',
    long_description = _LONG_DESCRIPTION,
    entry_points = {
        'console_scripts': [
            'dalmatian = dalmatian.fctools:main'
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
