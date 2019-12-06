"""Setup for cururu package."""
import os
import setuptools
import cururu

NAME = "cururu"


VERSION = cururu.__version__


AUTHOR = 'Davi Pereira dos Santos'


AUTHOR_EMAIL = ''


DESCRIPTION = 'Package for persistence for data science (Pajé, Oka, ...)'


with open('README.md', 'r') as fh:
    LONG_DESCRIPTION = fh.read()


LICENSE = 'GPL3'


URL = 'https://github.com/automated-data-science/cururu'


DOWNLOAD_URL = 'https://github.com/automated-data-science/cururu/releases'


CLASSIFIERS = ['Intended Audience :: Science/Research',
               'Intended Audience :: Developers',
               'License :: OSI Approved :: GPL3 License',
               'Natural Language :: English',
               'Programming Language :: Python',
               'Topic :: Software Development',
               'Topic :: Scientific/Engineering',
               'Operating System :: OS Independent',
               'Programming Language :: Python :: 3.6',
               'Programming Language :: Python :: 3.7']


INSTALL_REQUIRES = [
    'zstd', 'lz4', 'liac-arff', 'pymysql', 'blosc'
]


EXTRAS_REQUIRE = {
    'code-check': [
        'pylint',
        'mypy'
    ],
    'tests': [
        'pytest',
        'pytest-cov',
    ],
    'docs': [
        'sphinx',
        'sphinx-gallery',
        'sphinx_rtd_theme',
        'numpydoc'
    ]
}


setuptools.setup(
    name=NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    license=LICENSE,
    url=URL,
    download_url=DOWNLOAD_URL,
    packages=setuptools.find_packages(),
    classifiers=CLASSIFIERS,
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
)

