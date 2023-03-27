# postgres server 14 on ubuntu 22.04 image
FROM ubuntu:jammy

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update
RUN apt-get upgrade -qy

#-------------------------------------------- Install postgresql server (22.03.23 version:14) ------------------------------------

RUN apt-get install -qy --no-install-recommends postgresql

USER postgres

RUN /etc/init.d/postgresql start && psql --command "ALTER USER postgres WITH PASSWORD 'post1234';" \
	&& createdb -O postgres db1 \
	&& /etc/init.d/postgresql stop \
	&& echo "host all  all    0.0.0.0/0  md5" >> /etc/postgresql/14/main/pg_hba.conf \
	&& echo "listen_addresses='*'" >> /etc/postgresql/14/main/postgresql.conf


#-------------------------------------------- Install Clickhouse ------------------

USER root

RUN apt-get install -y apt-transport-https ca-certificates dirmngr
RUN GNUPGHOME=$(mktemp -d) && GNUPGHOME="$GNUPGHOME" gpg --no-default-keyring --keyring /usr/share/keyrings/clickhouse-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 8919F6BD2B48D754 \
	&& rm -r "$GNUPGHOME" \
	&& chmod +r /usr/share/keyrings/clickhouse-keyring.gpg

RUN echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" | tee \
    /etc/apt/sources.list.d/clickhouse.list
RUN apt-get update

RUN apt-get install -y clickhouse-server clickhouse-client


#-------------------------------------------- Install XDBC and prerequisites -------------------------------------------

RUN apt update && apt upgrade -qy

RUN apt-get install -y libabsl-dev libpq-dev libpqxx-dev

RUN apt install -qy clang libboost-all-dev libabsl-dev

#RUN apt-get update \
#  && apt-get -y install build-essential libzstd-dev \
#  && apt-get install -y wget \
#  && rm -rf /var/lib/apt/lists/* \
#  && wget https://github.com/Kitware/CMake/releases/download/v3.22.1/cmake-3.22.1-linux-x86_64.sh \
#      -q -O /tmp/cmake-install.sh \
#      && chmod u+x /tmp/cmake-install.sh \
#      && mkdir /opt/cmake-3.22.1 \
#      && /tmp/cmake-install.sh --skip-license --prefix=/opt/cmake-3.22.1 \
#      && rm /tmp/cmake-install.sh \
#      && ln -s /opt/cmake-3.22.1/bin/* /usr/local/bin

RUN apt install -qy cmake build-essential libzstd-dev

RUN mkdir /xdbc-server

COPY * /xdbc-server/

RUN mkdir /xdbc-server/build

WORKDIR /xdbc-server/build

RUN cmake ..

RUN make

#------------------------------------------------------------------------

USER postgres

ENV PATH /usr/lib/postgresql/14/bin:$PATH

EXPOSE 5432

VOLUME /var/lib/postgresql/data

CMD ["postgres","-D","/var/lib/postgresql/14/main","-c","config_file=/etc/postgresql/14/main/postgresql.conf"] 

