#!/bin/bash
set -x
#params
#$1 docker container name
CONTAINER=$1
#$2 1: build & run, 2: only run
OPTION=$2
#$3 run params (for now compression library)
RUNPARAMS=$3

if [[ $OPTION == 1 ]]; then
  #copy files
  for filetype in cpp h txt hpp; do
    for file in ../*.$filetype; do
      docker cp $file $CONTAINER:/xdbc-server/
      done
    done
  #build
  docker exec -it $CONTAINER bash -c "cd xdbc-server && rm -rf build/ && mkdir build && cd build && cmake .. && make -j8"

fi

# start
docker exec -it $CONTAINER bash -c "cd xdbc-server/build && ./xdbc-server ${RUNPARAMS}"
