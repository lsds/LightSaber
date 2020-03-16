#!/bin/bash

echo "Start installing..."

cd $HOME/LightSaber
mkdir -p build
cd build
#cmake .. -DCMAKE_BUILD_TYPE=Debug -DBENCHMARK_DOWNLOAD_DEPENDENCIES=ON
cmake .. -DCMAKE_BUILD_TYPE=Release -DBENCHMARK_DOWNLOAD_DEPENDENCIES=ON
make -j$(nproc)

# Run example
#cd $HOME/LightSaber/build/benchmarks/microbenchmarks
#./TestProjection

echo "All done..."