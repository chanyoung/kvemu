#!/bin/bash

DIR="/root/kv/FEMU/log/motivation"

function run_kv {
    local type_ssd=$1
    local write_ratio=$2

    # 1. Flush all data
    redis-cli flushall

    # 2. Start VM and logging
    ./run-${type_ssd}.sh > ${DIR}/${type_ssd}_${write_ratio}.txt 2>&1 &
    sleep 120

    # 3. Start tasks
    ssh -t -l liu localhost -p 8080 "cd uNVMe/; sudo NRHUGE=256 ./script/setup.sh"
    echo "[${type_ssd}_w:${write_ratio}%] setup done" 
    ssh -t -l liu localhost -p 8080 "cd uNVMe/app/fio_plugin; sudo ./fio-3.3 motivation_load.fio 2>&1"
    echo "[${type_ssd}_w:${write_ratio}%] load done" 
    ssh -t -l liu localhost -p 8080 "cd uNVMe/app/fio_plugin; sudo ./fio-3.3 motivation_ramp.fio 2>&1"
    echo "[${type_ssd}_w:${write_ratio}%] ramp done" 
    ssh -t -l liu localhost -p 8080 "cd uNVMe/app/fio_plugin; sudo ./fio-3.3 motivation_run_${write_ratio}.fio 2>&1"
    echo "[${type_ssd}_w:${write_ratio}%] run done" 
    ssh -t -l liu localhost -p 8080 "sudo shutdown -h now"
    sleep 60
}

run_kv "pink" "10"
run_kv "hash" "10"
run_kv "pink" "20"
run_kv "hash" "20"
run_kv "pink" "30"
run_kv "hash" "30"
