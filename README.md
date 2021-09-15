# python-otter - The post-processing package for [Otter](https://github.com/adamtuft/otter)

This is the post-processing package for use with Otter - please see [the main Otter repo](https://github.com/adamtuft/otter) for details.

## Getting Started

### Prerequisites

The following dependencies should be installed before installing `python-otter`:

- [OTF2 v2.3](https://zenodo.org/record/4682684)
- [`python-igraph` v0.9.1](https://pypi.org/project/python-igraph/0.9.1/)

### Installing python-otter

To install `python-otter` from github:

```bash
git clone https://github.com/adamtuft/python-otter.git && cd python-otter
git checkout main
pip install .
```

### Using python-otter

A trace recorded by Otter can be converted into a graph in `graph.dot` with:

```bash
python3 -m otter my-otter-trace/my-otter-trace.otf2 -o graph.dot
```

The graph, saved to `graph.dot`, can then be visualised using the `dot` command line tool included with [Graphviz](https://graphviz.org/) or a graph visualisation tool such as [yEd-Desktop or yEd-Live](https://www.yworks.com/\#products).

## Future Work

The future direction of development may include, in no particular order:

- [ ] Visualise actual work done per task.
- [ ] Automatic detection of the critical path.
- [ ] Support for MPI+OpenMP applications.
- [ ] Support for GPU-offloaded tasks.
- [ ] Stronger graph visualisation capabilities.

## Contributing

Contributions are welcome! If you would like to contribute, please fork the repository and use the `contributions` branch. There is no specific style guide, although I would be grateful if you could code in a style consistent with that of the main project.

## Issues, Questions and Feature Requests

Please post any of the above [in the main Otter repo](https://github.com/adamtuft/otter/issues) so as to keep everything in one place.

## Licensing

python-otter is released under the BSD 3-clause license. See [LICENSE](LICENSE) for details.

Copyright (c) 2021, Adam Tuft
All rights reserved.

## Acknowledgements

Otter was conceived and developed as the subject of a final project and dissertation for the the [Scientific Computing and Data Analysis MSc](https://miscada.phyip3.dur.ac.uk/) (MISCADA) at Durham University, UK. The author is grateful for the guidance and support of his supervisor Prof. Tobias Weinzierl and for the invaluable assistance provided by Dr. Holger Schulz.
