# postgres server 14 on ubuntu 22.04 image
FROM ubuntu:jammy

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update
RUN apt-get upgrade -qy

#-------------------------------------------- Install XDBC and prerequisites -------------------------------------------

RUN echo test

RUN apt update && apt upgrade -qy

RUN apt-get install -y libabsl-dev libpq-dev libpqxx-dev liblzo2-dev liblz4-dev libsnappy-dev

RUN apt install -qy clang libboost-all-dev libabsl-dev

RUN apt install -qy cmake build-essential libzstd-dev

RUN apt install -qy git

RUN git clone https://github.com/ClickHouse/clickhouse-cpp.git
RUN mkdir clickhouse-cpp/build
WORKDIR /clickhouse-cpp/build 
RUN cmake .. 
RUN make install
WORKDIR /

RUN git clone https://github.com/google/cityhash.git
WORKDIR /cityhash
RUN ./configure
RUN make all check CXXFLAGS="-g -O3"
RUN make install

RUN mkdir /xdbc-server

COPY * /xdbc-server/

RUN mkdir /xdbc-server/build

WORKDIR /xdbc-server/build

RUN cmake ..

RUN make

