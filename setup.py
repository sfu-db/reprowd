from setuptools import setup, find_packages

setup(
    name='reprowd',
    version='1.0.0',
    packages=find_packages(exclude=['*.docs','*.tests','*.exmaples']),
    author='SFU DB LABS',
    description='reprowd is a library to make crowdsourcing more convenient',
    author_email='jnwang@sfu.ca',
    url='https://github.com/sfu-db/reprowd'
)
