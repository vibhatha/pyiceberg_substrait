# Getting Started
## Get the repo
Fork and clone the repo.
```
git clone --recursive https://github.com/vibhatha/pyiceberg_substrait.git
cd pyiceberg-substrait
```
## Update the substrait submodule locally
This might be necessary if you are updating an existing checkout.
```
git submodule sync --recursive
git submodule update --init --recursive
```
## Upgrade the substrait submodule
You will need to regenerate protobuf classes if you do this (run `gen_proto.sh`).
```
cd third_party/substrait
git checkout <version>
cd -
git commit . -m "Use submodule <version>"
```


# Setting up your environment
## Conda env
Create a conda environment with developer dependencies.
```
conda env create -f environment.yml
conda activate substrait-iceberg-python-env
```

# Build
## Python package
Editable installation.
```
pip install -e .
```

# Test
Run tests in the project's root dir.
```
pytest
```

# Reference

Adopted Format from: https://github.com/substrait-io/substrait-python