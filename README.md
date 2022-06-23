# OPERA Calval Database

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This repository contains tools needed to interface with the OPERA calval database as well as example notebooks demonstrating database functionality

# Repository Organization

1. This reopository is split into 5 main directories according to OPERA product type.
2. Each main directory contains two folders:
    i.  tools folder contains scripts needed to interface with specific product database
    ii. examples folder contains jupyter notebooks that demonstrate database functionality (e.g. reading and downloading items, submitting new items to             database)


# Installation

Anaconda python and then install [mamba](https://github.com/mamba-org/mamba). Set up the environment.

```
mamba env create -f environment.yml
```

Then, `conda activate calval_database` and install the kernel for jupyter with `python -m ipykernel install --user --name
calval_database`.

# Database Organization
In Progress

# Amazon AWS Credentials
In Progress
