# postgres server 14 on ubuntu 22.04 image
FROM ubuntu:jammy

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update
RUN apt-get upgrade -qy

#-------------------------------------------- Install XDBC and prerequisites -------------------------------------------

RUN apt update && apt upgrade -qy

RUN apt-get install -y libabsl-dev libpq-dev libpqxx-dev liblzo2-dev liblz4-dev

RUN apt install -qy clang libboost-all-dev libabsl-dev

RUN apt install -qy cmake build-essential libzstd-dev

RUN mkdir /xdbc-server

COPY * /xdbc-server/

RUN mkdir /xdbc-server/build

WORKDIR /xdbc-server/build

RUN cmake ..

RUN make

