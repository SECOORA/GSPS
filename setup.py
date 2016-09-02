#!/usr/bin/env python

from distutils.core import setup

setup(
    name='gsps',
    version='1.0',
    author='Michael Lindemuth',
    author_email='mlindemu@usf.edu',
    packages=['gsps'],
    install_requires=[
        'gbdr',
        'pyinotify',
        'pyzmq',
    ],
    scripts=[
        'gsps/cli.py',
    ]
)
