from setuptools import find_packages, setup

version = ("0", "2", "0")

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(name="pyotter",
    version=".".join(version),
    author="Adam Tuft",
    author_email='adam.s.tuft@durham.ac.uk',
    description="Otter post-processing tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    platforms=["linux"],
    license="https://github.com/Otter-Taskification/pyotter/blob/dev/LICENSE",
    url="https://github.com/Otter-Taskification/pyotter",
    packages=find_packages(),
    package_data={'otter': ['templates/*', 'log/config/*.yaml']},
    install_requires=[
        'pyyaml>=6.0',
        'python-igraph==0.9.1',
        'loggingdecorators>=0.1.3'
    ],
    dependency_links=['https://perftools.pages.jsc.fz-juelich.de/cicd/otf2/']
)
