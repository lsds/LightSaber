#!/bin/bash

echo "Start installing..."

cd

sudo apt-get update
sudo apt-get upgrade

# Stuff
sudo apt-get install g++ build-essential python-dev autotools-dev libicu-dev libbz2-dev aptitude \
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

# Boost
sudo apt-get install libboost-all-dev

# TBB
sudo apt-get install libtbb-dev

# KAIO
sudo apt-get install libaio-dev

# IBVerbs
sudo apt install libibverbs-dev
#sudo apt-get install libmlx4–1 infiniband-diags ibutils ibverbs-utils rdmacm-utils

# Used for disk profiling
sudo apt install bpfcc-tools
# sudo biolatency-bpfcc -D 30 1
sudo apt install sysstat
# iostat -dx 30 2 (iostat -dx 1)  // mpstat 30 1 (cpu) // sar -n DEV 30 1 (network)
sudo apt install fio
# sudo fio --name=writefile --size=1G --filesize=1G --filename=/home/grt17/dat --bs=1M --nrfiles=1 --direct=1 --sync=0 --randrepeat=0 --rw=write --refill_buffers --end_fsync=1 --iodepth=200 --ioengine=libaio

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
make -j$(nproc)
sudo make install
echo 'export PATH=/usr/lib/ccache:$PATH' >> $HOME/.profile
sudo ln -s /usr/local/bin/doxygen /usr/bin/doxygen
cd

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
make -j$(nproc)
sudo make install
echo "export LLVM_HOME=$(pwd)" >> $HOME/.profile
echo 'export PATH=$LLVM_HOME/bin:$PATH' >> $HOME/.profile
echo 'export LIBRARY_PATH=$LLVM_HOME/lib:$LIBRARY_PATH' >> $HOME/.profile
source $HOME/.profile
sudo rm /etc/ld.so.cache
sudo ldconfig
sudo ln -s /usr/local/bin/clang++ /usr/lib/ccache/clang++
sudo ln -s /usr/local/bin/clang /usr/lib/ccache/clang
cd

# Google Test
sudo apt-get install libgtest-dev
cd /usr/src/gtest
sudo cmake CMakeLists.txt
sudo make -j$(nproc)
# cd /usr/src/googletest
# sudo cmake CMakeLists.txt
# sudo make -j$(nproc)
# sudo cp googlemock/gtest/*a /usr/lib
# sudo cp ./lib/*.a /usr/lib
sudo cp *.a /usr/lib/
sudo mkdir /usr/local/lib/gtest
sudo ln -s /usr/lib/libgtest.a /usr/local/lib/gtest/libgtest.a
sudo ln -s /usr/lib/libgtest_main.a /usr/local/lib/gtest/libgtest_main.a
cd

# Google Benchmark
git clone https://github.com/google/benchmark.git
cd benchmark
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=RELEASE -DBENCHMARK_DOWNLOAD_DEPENDENCIES=ON
make -j$(nproc)
sudo make install
cd

# PMDK
sudo apt install -y git gcc g++ autoconf automake asciidoc asciidoctor bash-completion xmlto libtool pkg-config libglib2.0-0 libglib2.0-dev libfabric1 libfabric-dev doxygen graphviz pandoc libncurses5 libkmod2 libkmod-dev libudev-dev uuid-dev libjson-c-dev libkeyutils-dev
git clone https://github.com/pmem/ndctl
cd ndctl
git checkout c7767834871f7ce50a2abe1da946e9e16fb08eda
sudo ./autogen.sh
sudo ./configure CFLAGS='-g -O2' --prefix=/usr/local --sysconfdir=/etc --libdir=/usr/local/lib64
#sudo ./configure CFLAGS='-g -O2' --prefix=/usr --sysconfdir=/etc --libdir=/usr/lib
sudo make -j$(nproc)
sudo make install
cd

sudo apt install autoconf automake pkg-config libglib2.0-dev libfabric-dev pandoc libncurses5-dev
git clone https://github.com/pmem/pmdk
cd pmdk
git checkout 3bc5b0da5a7a5d5752ad2cb4f5f9bf0edfd47d67
export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:${PKG_CONFIG_PATH}
make -j$(nproc)
sudo PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:${PKG_CONFIG_PATH} make install
sudo sh -c "echo /usr/local/lib >> /etc/ld.so.conf"
sudo sh -c "echo /usr/local/lib64 >> /etc/ld.so.conf"
sudo ldconfig
cd
# PKG_CONFIG_PATH
echo 'export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:${PKG_CONFIG_PATH}' >> $HOME/.profile
source $HOME/.profile


git clone https://github.com/pmem/libpmemobj-cpp.git
cd libpmemobj-cpp
git checkout 9f784bba07b94cd36c9eebeaa88c5df4f05045b2
mkdir build
cd build
cmake -DTESTS_USE_VALGRIND=OFF ..
make -j$(nproc)
sudo make install
cd

## Set up home directory
# echo "Setting up the home directory in src/utils/SystemConf.cpp"
# sed -i '65s#.*#"'$HOME'"#' $HOME/LightSaber/src/CMakeLists.txt

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
