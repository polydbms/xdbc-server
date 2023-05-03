# postgres server 14 on ubuntu 22.04 image
FROM ubuntu:jammy

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update
RUN apt-get upgrade -qy

#-------------------------------------------- Install XDBC and prerequisites -------------------------------------------

# install dependencies

RUN apt install -qy clang libboost-all-dev cmake build-essential git

# install compression libs

RUN apt install -qy libzstd-dev liblzo2-dev liblz4-dev libsnappy-dev

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

COPY * /xdbc-server/

RUN rm -rf /xdbc-server/build && mkdir /xdbc-server/build && cd /xdbc-server/build && cmake .. && make -j8

#WORKDIR /xdbc-server/build

#RUN cmake ..

#RUN make

ENTRYPOINT ["tail", "-f", "/dev/null"]