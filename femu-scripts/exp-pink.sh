#!/bin/bash

set -e

function run {
    algorithm=$1;
    workload=$2;
    write=$3;

    filename=${workload}.${write}.${algorithm};

    ./run-${algorithm}.sh > ${filename}.txt 2>&1 &
    sleep 90

    ssh -l liu localhost -p 8080 "cd uNVMe/; sudo NRHUGE=256 ./script/setup.sh &>/dev/null"
    echo "[PHASE BOOT] Workload: ${workload} / Write ratio: ${write} / Algorithm: ${algorithm}"

    start=$(date +%s)
    ssh -l liu localhost -p 8080 "cd uNVMe/app/fio_plugin; sudo ./fio-3.3 workloads/${workload}_load.fio &>~/log/${filename}.load"
    end=$(date +%s)
    elapsed=$((end - start))
    eval "echo [PHASE LOAD] Done. $(date -ud "@$elapsed" +'$((%s/3600/24)) days %H hr %M min %S sec')"

    start=$(date +%s)
    ssh -l liu localhost -p 8080 "cd uNVMe/app/fio_plugin; sudo ./fio-3.3 workloads/${workload}_ramp.fio &>~/log/${filename}.ramp"
    end=$(date +%s)
    elapsed=$((end - start))
    eval "echo [PHASE RAMP] Done. $(date -ud "@$elapsed" +'$((%s/3600/24)) days %H hr %M min %S sec')"

    start=$(date +%s)
    ssh -l liu localhost -p 8080 "cd uNVMe/app/fio_plugin; sudo ./fio-3.3 workloads/${workload}_run_${write}.fio &>~/log/${filename}.run"
    end=$(date +%s)
    elapsed=$((end - start))
    eval "echo [PHASE RUN] Done. $(date -ud "@$elapsed" +'$((%s/3600/24)) days %H hr %M min %S sec')"

    ssh -l liu localhost -p 8080 "sleep 2; sudo shutdown -h now &"
    sleep 60
}

#run "lsmtree" "zippydb" "20"
run "lksv" "zippydb" "20"
run "pink" "zippydb" "20"

#run "pink" "pink" "20"
#run "lsmtree" "pink" "20"
#run "lksv" "pink" "20"
