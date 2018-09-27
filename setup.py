import re
from setuptools import setup, find_packages

version = re.search(
    r'__version__\s*=\s*[\'"]([^\'"]+)[\'"]',
    open('openprocurement/patchdb/patcher.py').read()
).group(1)

requires = [
    'couchdb-schematics',
    'iso8601',
    'jsonpatch',
    'pytz',
    'python-cjson',
    'requests',
    'setuptools',
    'simplejson'
]

entry_points = {
    'console_scripts': [
        'patchdb=openprocurement.patchdb.main:main',
    ]
}

setup(
    name='openprocurement.patchdb',
    version=version,
    description="Command line tool for patch tender documets",
    long_description=open("README.md").read(),
    # Get more strings from
    # http://pypi.python.org/pypi?:action=list_classifiers
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
    ],
    platforms=['posix'],
    keywords='openprocurement',
    author='Volodymyr Flonts',
    author_email='flyonts@gmail.com',
    url='https://github.com/imaginal/openprocurement.patchdb',
    license='Apache License 2.0',
    packages=find_packages(),
    namespace_packages=['openprocurement'],
    include_package_data=True,
    zip_safe=False,
    install_requires=requires,
    entry_points=entry_points
)
