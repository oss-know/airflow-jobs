#!/bin/bash
SRC_HOST=$2
indice=($1)
input_path=$3
params_count=$#
export NODE_TLS_REJECT_UNAUTHORIZED=0
echo '索引:' "${indice[@]}"
day=$(date "+%Y-%m-%d")
date_time=$(date "+%Y-%m-%dT%H:%M:%SZ%z")
for index in "${indice[@]}"
do
echo $index
 elasticdump --input=${input_path} \
       --output=https://admin:admin@${SRC_HOST}/${index} \
       --limit=5000 \
       --type=data \
       --fsCompress &
done
wait