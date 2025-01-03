# postgres server 14 on ubuntu 22.04 image
FROM ubuntu:jammy

ENV DEBIAN_FRONTEND=noninteractive

# Update apt and install necessary tools
RUN apt-get update && apt-get upgrade -qy \
    && apt-get install -qy \
    ca-certificates \
    lsb-release \
    wget \
    curl \
    gnupg2 \
    apt-transport-https \
    && rm -rf /var/lib/apt/lists/*

# Use an alternative mirror in case the default is unavailable
RUN sed -i 's/http:\/\/archive.ubuntu.com/http:\/\/mirrors.kernel.org/' /etc/apt/sources.list \
    && apt-get update

# install arrow/parquet dependencies
RUN wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && rm -f apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

RUN apt-get update && apt install -y --no-install-recommends \
    libarrow-dev \
    libparquet-dev

# install other dependencies
RUN apt install -qy \
    clang \
    libboost-all-dev \
    cmake \
    build-essential \
    git \
    libspdlog-dev \
    gdb \
    nlohmann-json3-dev \
    iproute2 \
    netcat \
    wget \
    libzstd-dev \
    liblzo2-dev \
    liblz4-dev \
    libsnappy-dev \
    libbrotli-dev \
    libpq-dev \
    libpqxx-dev \
    libabsl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install compression libs
RUN git clone https://github.com/lemire/FastPFor.git && cd FastPFor && \
    mkdir build && \
    cd build && \
    cmake .. && \
    cmake --build . && \
    make install && cd ../..

RUN git clone https://github.com/LLNL/fpzip.git && cd fpzip && \
    mkdir build && \
    cd build && \
    cmake .. && \
    cmake --build . --config Release && \
    make install && cd ../..

# install clickhouse dependencies
RUN git clone https://github.com/google/cityhash.git && cd /cityhash && \
    ./configure && make all check CXXFLAGS="-g -O3" && make install && cd ..

RUN git clone https://github.com/ClickHouse/clickhouse-cpp.git && cd /clickhouse-cpp && \
    rm -rf build && mkdir build && cd build && cmake .. -DWITH_SYSTEM_ABSEIL=ON && \
    make -j8 && make install && cd ../..

# copy and install xdbc server
RUN mkdir /xdbc-server
ADD CMakeLists.txt /xdbc-server/
ADD customQueue.h /xdbc-server/
ADD main.cpp /xdbc-server/
ADD xdbcserver.cpp /xdbc-server/
ADD xdbcserver.h /xdbc-server/
ADD metrics_calculator.h /xdbc-server/
ADD Compression /xdbc-server/Compression
ADD DataSources /xdbc-server/DataSources
ADD ControllerInterface /xdbc-server/ControllerInterface
RUN ls /xdbc-server

# Prepare build environment
RUN rm -rf  /xdbc-server/CMakeCache.txt
RUN rm -rf /xdbc-server/build && mkdir /xdbc-server/build && cd /xdbc-server/build && cmake .. -D CMAKE_BUILD_TYPE=Release && make -j8

ENTRYPOINT ["tail", "-f", "/dev/null"]
