"""Setup for cururu package."""
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
               'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
               'Natural Language :: English',
               'Programming Language :: Python',
               'Topic :: Scientific/Engineering',
               'Operating System :: Linux',
               'Programming Language :: Python :: 3.8']


INSTALL_REQUIRES = [
    'numpy', 'sklearn', 'liac-arff'
]


EXTRAS_REQUIRE = {
}

SETUP_REQUIRES = ['wheel']

setuptools.setup(
    name=NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    classifiers=CLASSIFIERS,
    description=DESCRIPTION,
    download_url=DOWNLOAD_URL,
    extras_require=EXTRAS_REQUIRE,
    install_requires=INSTALL_REQUIRES,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    license=LICENSE,
    packages=setuptools.find_packages(),
    setup_requires=SETUP_REQUIRES,
    url=URL,
)

package_dir = {'': 'cururu'}  # For IDEs like Intellij to recognize the package.

