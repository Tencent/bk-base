#!/usr/bin/env bash

pip install sphinx_rtd_theme

make clean
sphinx-apidoc -o docs/ metadata_client -f

# shellcheck disable=SC2164
cd docs
cp modules.rst.bak modules.rst
shopt -s extglob
rm !(metadata_client.rst|modules.rst|modules.rst.bak)

# shellcheck disable=SC2103
cd ..
make html
