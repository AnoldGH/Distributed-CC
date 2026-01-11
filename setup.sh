#!/bin/sh
mkdir external

# TODO: Pull submodules
cd external
cd constrained-clustering
./setup.sh
./easy_build_and_compile.sh