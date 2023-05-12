#!/bin/bash
set -x
#params
#$1 docker container name
CONTAINER=$1
#$2 1: build & run, 2: only run, 3: only build
OPTION=$2
#$3 run params (for now compression library)
RUNPARAMS=$3

if [ $OPTION == 1 ] || [ $OPTION == 3 ]; then
  DIR=$(dirname $(dirname "$(realpath -- "$0")"))
  docker exec $CONTAINER bash -c "rm -rf xdbc-server && mkdir xdbc-server"
  #copy files
  for filetype in cpp h txt hpp; do
    for file in ${DIR}/*.$filetype; do
      docker cp $file $CONTAINER:/xdbc-server/
    done
  done
  #build
  docker exec $CONTAINER bash -c "cd xdbc-server && rm -rf build/ && mkdir build && cd build && cmake .. && make -j8"

fi

# start
if [[ $OPTION != 3 ]]; then
  docker exec $CONTAINER bash -c "cd xdbc-server/build && ./xdbc-server ${RUNPARAMS}"
fi
