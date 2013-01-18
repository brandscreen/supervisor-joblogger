#! /usr/bin/env python

from distutils.core import setup

setup(name='supervisor-joblogger',
      version='1.0',
      description='Provides a plugin to monitor and log jobs via supervisor',
      license='BSD',
      author='Seth Yates',
      author_email='syates@brandscreen.com',
      url='https://github.com/brandscreen/supervisor-joblogger',
      scripts=['joblogger.py'])
