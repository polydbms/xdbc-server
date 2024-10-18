# postgres server 14 on ubuntu 22.04 image
FROM ubuntu:jammy

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update
RUN apt-get upgrade -qy

#-------------------------------------------- Install XDBC and prerequisites -------------------------------------------

# install dependencies

RUN apt install -qy clang libboost-all-dev cmake build-essential git libspdlog-dev gdb nlohmann-json3-dev iproute2 netcat

#RUN git clone https://github.com/gabime/spdlog.git && cd spdlog && mkdir build && cd build &&  cmake .. && make -j && make install

# install compression libs

RUN apt install -qy libzstd-dev liblzo2-dev liblz4-dev libsnappy-dev libbrotli-dev

#RUN git clone https://github.com/LLNL/zfp.git && cd zfp && make

RUN git clone https://github.com/lemire/FastPFor.git && cd FastPFor && \
    mkdir build && \
    cd build && \
    cmake .. && \
    cmake --build . && \
    make install

RUN git clone https://github.com/LLNL/fpzip.git && cd fpzip && \
    mkdir build && \
    cd build && \
    cmake .. && \
    cmake --build . --config Release && \
    make install

# install postgres dependencies

RUN apt install -qy libpq-dev libpqxx-dev

# install clickhouse depencencies

RUN apt-get install -y libabsl-dev

RUN git clone https://github.com/google/cityhash.git

RUN cd /cityhash && ./configure && make all check CXXFLAGS="-g -O3" && make install

# install clickhouse-lib

RUN git clone https://github.com/ClickHouse/clickhouse-cpp.git

RUN cd /clickhouse-cpp && rm -rf build && mkdir build && cd build && cmake .. -DWITH_SYSTEM_ABSEIL=ON && make -j8 && make install

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
RUN ls /xdbc-server

RUN rm -rf  /xdbc-server/CMakeCache.txt
RUN rm -rf /xdbc-server/build && mkdir /xdbc-server/build && cd /xdbc-server/build && cmake .. -D CMAKE_BUILD_TYPE=Release && make -j8

ENTRYPOINT ["tail", "-f", "/dev/null"]