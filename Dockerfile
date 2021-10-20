FROM ubuntu:bionic

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt update && \
    apt upgrade -y && \
		apt install -y \
		aptitude \
		autotools-dev \
		binutils-dev \
		bison \
		build-essential \
		ccache \
		flex \
		gcc  \
		g++  \
		git \
		libboost-all-dev \
		libbz2-dev \
		libdouble-conversion-dev \
		libevent-dev \
		libffi-dev \
		libgflags-dev \
		libgoogle-glog-dev \
		libgtest-dev \
		libiberty-dev \
		libicu-dev \
		libjemalloc-dev \
		liblz4-dev \
		liblzma-dev \
		libsnappy-dev \
		libssl-dev \
		libtbb-dev \
		libxml2-dev \
		make \
		pkg-config \
		python-dev \
		zlib1g-dev \
		wget \
		libaio-dev \
		libibverbs-dev \
		bpfcc-tools \
		sysstat \
		fio

RUN cd && \
    apt remove --purge --auto-remove cmake && \
    version=3.16 && \
    build=2 && \
    mkdir ~/temp && \
    cd ~/temp && \
    wget https://cmake.org/files/v$version/cmake-$version.$build.tar.gz && \
    tar -xzvf cmake-$version.$build.tar.gz && \
    cd cmake-$version.$build/ && \
    ./bootstrap && \
    make -j$(nproc) && \
    make install

RUN cd /usr/src/gtest && \
    cmake . && \
		make && \
		cp *.a /usr/lib && \
		mkdir -p /usr/local/lib/gtest && \
		ln -s /usr/lib/libgtest.a /usr/local/lib/gtest/libgtest.a && \
		ln -s /usr/lib/libgtest_main.a /usr/local/lib/gtest/libgtest_main.a

RUN cd && \
    git clone --depth 1 https://github.com/google/benchmark.git && \
		cd benchmark && \
		mkdir build && \
		cd build && \
		cmake .. -DCMAKE_BUILD_TYPE=Release -DBENCHMARK_DOWNLOAD_DEPENDENCIES=ON && \
		make -j$(nproc) && \
		make install

RUN cd && \
    git clone https://github.com/llvm/llvm-project.git && \
		cd llvm-project && \
		git checkout e3a94ba4a92 && \
		mkdir build && \
		cd build && \
		cmake -DLLVM_ENABLE_PROJECTS=clang -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=ON -DCLANG_INCLUDE_DOCS=OFF -DCLANG_INCLUDE_TESTS=OFF \
    -DCLANG_INSTALL_SCANBUILD=OFF -DCLANG_INSTALL_SCANVIEW=OFF -DCLANG_PLUGIN_SUPPORT=OFF \
    -DLLVM_TARGETS_TO_BUILD=X86 -G "Unix Makefiles" ../llvm && \
		make -j$(nproc) && \
		make install

RUN ln -s /root/llvm-project/build/bin/clang++ /usr/lib/ccache/ && \
    ln -s /root/llvm-project/build/bin/clang /usr/lib/ccache/


ENV LLVM_HOME=/root/llvm-project/build
ENV PATH=$LLVM_HOME/bin:$PATH
ENV LIBRARY_PATH=$LLVM_HOME/lib:$LIBRARY_PATH
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LIBRARY_PATH
ENV PATH=/usr/lib/ccache:$PATH

RUN cd && \
    apt install -y \
    git  \
    gcc  \
    g++  \
    autoconf  \
    automake  \
    asciidoc  \
    asciidoctor  \
    bash-completion  \
    xmlto  \
    libtool  \
    pkg-config  \
    libglib2.0-0  \
    libglib2.0-dev  \
    libfabric1  \
    libfabric-dev  \
    doxygen  \
    graphviz  \
    pandoc  \
    libncurses5  \
    libkmod2  \
    libkmod-dev  \
    libudev-dev  \
    uuid-dev  \
    libjson-c-dev  \
    libkeyutils-dev \
    systemd \
    libsystemd-dev

RUN cd && \
    git clone https://github.com/pmem/ndctl && \
    cd ndctl && \
    git checkout c7767834871f7ce50a2abe1da946e9e16fb08eda && \
    ./autogen.sh && \
    ./configure CFLAGS='-g -O2' --prefix=/usr/local --sysconfdir=/etc --libdir=/usr/local/lib64 && \
    make -j$(nproc) && \
    make install

RUN cd && \
    apt install -y \
    autoconf  \
    automake  \
    pkg-config  \
    libglib2.0-dev  \
    libfabric-dev  \
    pandoc  \
    libncurses5-dev

RUN cd && \
    git clone https://github.com/pmem/pmdk && \
    cd pmdk && \
    git checkout 3bc5b0da5a7a5d5752ad2cb4f5f9bf0edfd47d67 && \
    export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:${PKG_CONFIG_PATH} && \
    make -j$(nproc) && \
    PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:${PKG_CONFIG_PATH} make install && \
    echo /usr/local/lib >> /etc/ld.so.conf && \
    echo /usr/local/lib64 >> /etc/ld.so.conf && \
    ldconfig
#    echo 'export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:${PKG_CONFIG_PATH}' >> $HOME/.profile && \
#    source $HOME/.profile

ENV PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:${PKG_CONFIG_PATH}

RUN cd && \
    git clone https://github.com/pmem/libpmemobj-cpp.git && \
    cd libpmemobj-cpp && \
    git checkout 9f784bba07b94cd36c9eebeaa88c5df4f05045b2 && \
    mkdir build && \
    cd build && \
    cmake -DTESTS_USE_VALGRIND=OFF .. && \
    make -j$(nproc) && \
    make install

RUN cd && \
    git clone https://github.com/lsds/LightSaber.git && \
    cd LightSaber && \
    ./scripts/build.sh