# pyotter - The post-processing package for [Otter](https://github.com/Otter-Taskification/otter)

This is the post-processing package for use with Otter - please see [the main Otter repo](https://github.com/Otter-Taskification/otter) for details.

## Getting Started

### Prerequisites

The following dependencies should be installed before installing pyotter:

- [OTF2 v2.3](https://zenodo.org/record/4682684)
- [`python-igraph` v0.9.1](https://pypi.org/project/python-igraph/0.9.1/)

### Installing pyotter

To install pyotter from github:

```bash
git clone https://github.com/Otter-Taskification/pyotter.git && cd pyotter
git checkout dev
pip install .
```

To install from PyPi:

```
pip install pyotter
```

### Using pyotter

To process an Otter trace with pyotter and produce a report:

```bash
python3 -m otter path/to/anchor-file.otf2 --report my-report-name
```

This will create a folder `./my-report-name` containing the report output. If the folder already exists, pyotter will fail unless the `--force` option is specified.

The `--logdir` and `--loglevel` options control where log files are created and the verbosity of logging.

## Contributing

Contributions are welcome! If you would like to contribute, please fork the repository and branch from the latest tag. There is no specific style guide, although I would be grateful if you could code in a style consistent with that of the main project.

## Licensing

pyotter is released under the BSD 3-clause license. See [LICENSE](LICENSE) for details.

Copyright (c) 2021, Adam Tuft
All rights reserved.

## Acknowledgements

pyotter was conceived and developed as the subject of a final project and dissertation for the the [Scientific Computing and Data Analysis MSc](https://miscada.phyip3.dur.ac.uk/) (MISCADA) at Durham University, UK. The author is grateful for the guidance and support of his supervisor Prof. Tobias Weinzierl and for the invaluable assistance provided by Dr. Holger Schulz.
