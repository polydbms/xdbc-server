#!/bin/bash
#set -x
#params
#$1 docker container name
CONTAINER=$1
#$2 1: build & run, 2: only run, 3: only build
BUILD_OPTION=$2
#$3 run params (for now compression library, parallelism)
RUNPARAMS=$3

if [ $BUILD_OPTION == 1 ] || [ $BUILD_OPTION == 3 ]; then
  DIR=$(dirname $(dirname "$(realpath -- "$0")"))
  docker exec $CONTAINER bash -c "rm -rf xdbc-server && mkdir xdbc-server"
  #copy files
  for filetype in cpp h txt hpp; do
    for file in ${DIR}/*.$filetype; do
      docker cp $file $CONTAINER:/xdbc-server/
    done
  done

  docker cp ${DIR}/Compression/ $CONTAINER:/xdbc-server/
  docker cp ${DIR}/DataSources/ $CONTAINER:/xdbc-server/

  #build
  docker exec $CONTAINER bash -c "cd xdbc-server && rm -rf build/ && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j8"

fi

rm /tmp/server_exec.log
# start
if [[ $BUILD_OPTION != 3 ]]; then
  #docker exec -t $CONTAINER bash -c "cd xdbc-server/build && ./xdbc-server ${RUNPARAMS}"
  docker exec $CONTAINER bash -c "cd xdbc-server/build && nohup ./xdbc-server ${RUNPARAMS}" >> /tmp/server_exec.log 2>&1 &
fi
