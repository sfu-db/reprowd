from setuptools import setup, find_packages

setup(
    name='reprowd',
    version='0.1',
    packages=find_packages(exclude=['*.docs','*.tests','*.exmaples']),
    author='SFU Database System Lab',
    description='Reprowd is a system that aims to make it easy to reproduce crowdsourced data processing research',
    author_email='jnwang@sfu.ca',
    url='http://sfu-db.github.io/reprowd/'
)
