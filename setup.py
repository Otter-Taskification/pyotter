from setuptools import find_packages, setup
from pyotter_version import version

setup(name="pyotter",
    version=".".join(version),
    description="Otter post-processing tool",
    author="Adam Tuft",
    author_email='adam.s.tuft@gmail.com',
    platforms=["linux"],
    license="https://github.com/adamtuft/python-otter/blob/main/LICENSE",
    url="https://github.com/adamtuft/python-otter",
    packages=find_packages(),
    install_requires=[
        'python-igraph==0.9.1'
    ],
    dependency_links=['https://perftools.pages.jsc.fz-juelich.de/cicd/otf2/']
)
