#!/usr/bin/env python
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
from setuptools import setup


def readme():
    with open('README.rst') as f:
        return f.read()


setup(name='HYBRID',
      version='0.5.1',
      author="Warren Davis",
      author_email="wldavis@sandia.gov",
      description='Hybrid tool suite',
      long_description=readme(),
      install_requires=['celery', 'BeautifulSoup==3.2.1', 'CouchDB==0.9', 'pytz==2014.4', 'pymongo==2.7.1', 'nltk'],
      packages=['hybrid', 'hybrid_titan', 'hybrid.worker'],
      scripts=['executors/hybrid_executor.py'],
      provides=["hybrid"],
      zip_safe=True
      )
