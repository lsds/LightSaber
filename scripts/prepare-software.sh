#!/bin/bash

echo "Start installing..."

cd

sudo apt-get update
sudo apt-get upgrade

# CMake 3.16
sudo apt remove --purge --auto-remove cmake
version=3.16
build=2
mkdir ~/temp
cd ~/temp
wget https://cmake.org/files/v$version/cmake-$version.$build.tar.gz
tar -xzvf cmake-$version.$build.tar.gz
cd cmake-$version.$build/
./bootstrap
make -j$(nproc)
sudo make install
cd

# CCache
sudo apt-get install ccache

# Doxygen
sudo apt-get install flex
sudo apt-get install bison
git clone https://github.com/doxygen/doxygen.git
cd doxygen
mkdir build
cd build
cmake -G "Unix Makefiles" ..
make
sudo make install
echo "export PATH=/usr/lib/ccache:$PATH" >> $HOME/.profile
sudo ln -s /usr/local/bin/doxygen /usr/bin/doxygen
cd

# Stuff
sudo apt-get install g++ build-essential g++ python-dev autotools-dev libicu-dev build-essential libbz2-dev aptitude \
    libevent-dev \
    libdouble-conversion-dev \
    libgoogle-glog-dev \
    libgflags-dev \
    libiberty-dev \
    liblz4-dev \
    liblzma-dev \
    libsnappy-dev \
    make \
    zlib1g-dev \
    binutils-dev \
    libjemalloc-dev \
    libssl-dev \
    pkg-config

# LLVM 9
git clone https://github.com/llvm/llvm-project.git
cd llvm-project
git checkout e3a94ba4a92
mkdir build
cd build
cmake -DLLVM_ENABLE_PROJECTS=clang -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=ON -DCLANG_INCLUDE_DOCS=OFF -DCLANG_INCLUDE_TESTS=OFF \
    -DCLANG_INSTALL_SCANBUILD=OFF -DCLANG_INSTALL_SCANVIEW=OFF -DCLANG_PLUGIN_SUPPORT=OFF \
    -DLLVM_TARGETS_TO_BUILD=X86 -G "Unix Makefiles" ../llvm
make
sudo make install
echo "export LLVM_HOME=$(pwd)" >> $HOME/.profile
echo "export PATH=$LLVM_HOME/bin:$PATH" >> $HOME/.profile
echo "export LIBRARY_PATH=$LLVM_HOME/lib:$LIBRARY_PATH" >> $HOME/.profile
sudo rm /etc/ld.so.cache
sudo ldconfig
cd

# Boost
sudo apt-get install libboost-all-dev

# TBB
sudo apt-get install libtbb-dev

# Google Test
sudo apt-get install libgtest-dev
cd /usr/src/gtest
sudo cmake CMakeLists.txt
sudo make
sudo cp *.a /usr/lib
sudo ln -s /usr/lib/libgtest.a /usr/local/lib/gtest/libgtest.a
sudo ln -s /usr/lib/libgtest_main.a /usr/local/lib/gtest/libgtest_main.a
cd

# Google Benchmark
git clone https://github.com/google/benchmark.git
cd benchmark
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=RELEASE -DBENCHMARK_DOWNLOAD_DEPENDENCIES=ON
make
sudo make install
cd

# Build LightSaber
#cd $HOME/LightSaber
#mkdir build
#cd build
#cmake -DCMAKE_BUILD_TYPE=Release -G "Unix Makefiles" ..
#make

# Run example
#cd $HOME/LightSaber/build/test/benchmarks/microbenchmarks/
#./TestProjection

echo "All done..."
