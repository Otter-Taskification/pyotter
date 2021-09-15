from setuptools import find_packages, setup
from pyotter_version import version

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(name="pyotter",
    version=".".join(version),
    author="Adam Tuft",
    author_email='adam.s.tuft@gmail.com',
    description="Otter post-processing tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    platforms=["linux"],
    license="https://github.com/adamtuft/pyotter/blob/main/LICENSE",
    url="https://github.com/adamtuft/pyotter",
    packages=find_packages(),
    install_requires=[
        'python-igraph==0.9.1'
    ],
    dependency_links=['https://perftools.pages.jsc.fz-juelich.de/cicd/otf2/']
)
