#!/usr/bin/env bash

function start_etcd {
    sudo zula
    cd /home/zula/etcd/bin
    ./etcd --quota-backend-bytes=107374182400 > log 2>&1 & 
    sleep 60
   cd /home/zula/etcd-ybd/tools/benchmark
}

function stop_etcd {
    pkill etcd
    rm -rf /home/zula/etcd/bin/default.etcd
    sleep 10
}

function start_ydb {
    sudo zula
    cd /mnt/vdb/ydbd

    ./start.sh disk
    sleep 40
    cd /home/zula/etcd-ydb/etcd-ydb-userver/build-release
    ./etcd-ydb-userver --config /home/zula/etcd-ydb/etcd-ydb-userver/configs/static_config.yaml --config_vars /home/zula/etcd-ydb/etcd-ydb-userver/configs/config_vars.yaml > log 2>&1 &
    sleep 30
    cd /home/zula/etcd-ydb/tools/benchmark
}

function stop_ydb {
    cd /mnt/vdb/ydbd
    ./stop.sh
    pkill etcd-ydb-userve
    sleep 30
    rm ydb.data
    sleep 10
   cd /home/zula/etcd-ybd/tools/benchmark
}


## $1: operation
## $2: total
## $3: txn-ops
function get_args {
    ETCDCTL_FLAGS="--rate-limit=1_000_000_000 --key-size=11_700 --val-size=11_700 --key-space-size=20_000_000_000 --total=$2"
    if [[ "$1" == "put" ]]; then
        echo "$ETCDCTL_FLAGS"
    elif [[ "$1" == "watch-put" ]]; then
        echo "$ETCDCTL_FLAGS"
    elif [[ "$1" == "range" ]]; then
        echo "$ETCDCTL_FLAGS"
    elif [[ "$1" == "mixed" ]]; then
        echo "$ETCDCTL_FLAGS --read-ratio=0.8"
    elif [[ "$1" == "txn-put" ]]; then
        echo "$ETCDCTL_FLAGS --txn-ops=$3"
    elif [[ "$1" == "txn-range" ]]; then
        echo "$ETCDCTL_FLAGS --txn-ops=$3"
    elif [[ "$1" == "txn-mixed" ]]; then
        echo "$ETCDCTL_FLAGS --txn-ops=$3 --read-ratio=0.8"
    fi
}

## $1: target
function endpoints {
    if [[ "$1" == "etcd" ]]; then
        echo "localhost:2379"
    elif [[ "$1" == "ydb" ]]; then
        echo "localhost:8081"
    fi
}

## $1: target
## $2: operation
## $3: total
## $4: txn-ops
## $5: counter
function run {
    echo "go run . --clients=10 --conns=1 --endpoints=$(endpoints $1) $2 $(get_args $2 $3 $4) > result/$1/$2/$4/$5.json"
    time  go run . --clients=10 --conns=1 --endpoints="$(endpoints $1)" "$2" $(get_args "$2" "$3" "$4") > "result/$1/$2/$4/$5.json"
}

## $1: target
## $2: operation
## $3: txn-ops
function fill {
    echo "START $1"
    start_$1

    mkdir -p result/$1/$2/$3/
    if [[ "$2" == "put" ]]; then
        mkdir -p result/$1/range/
    elif [[ "$2" == "txn-put" ]]; then
        mkdir -p result/$1/txn-range/$3/
    fi

    COUNTER=1
    for TOTAL in "40_000" "40_000" "40_000" "40_000" "40_000" "40_000" "40_000" "40_000" "40_000" "40_000" "40_000" "40_000" "40_000" "40_000" "40_000" "40_000"; do
        run "$1" "$2" "$TOTAL" "$3" "$COUNTER"
        if [[ "$2" == "put" ]]; then
            run "$1" "range" "$TOTAL" "$3" "$COUNTER"
        elif [[ "$2" == "txn-put" ]]; then
            run "$1" "txn-range" "$TOTAL" "$3" "$COUNTER"
        fi
        if [[ "$1" == "etcd" ]]; then
            echo "etcdctl --endpoints=$(endpoint $1) endpoint status -w table"
            etcdctl --endpoints="$(endpoint $1)" endpoint status -w table
        fi
        COUNTER=$(($COUNTER +1))
    done

    echo "STOP $1"
    stop_$1
}

function main() {
    for TARGET in ydb etcd; do
        for OPERATION in txn-put txn-mixed put watch-put mixed; do
            if [[ $OPERATION == "put" ]]; then
                fill $TARGET $OPERATION
            elif [[ $OPERATION == "watch-put" ]]; then
                fill $TARGET $OPERATION
            elif [[ $OPERATION == "mixed" ]]; then
                fill $TARGET $OPERATION
            elif [[ $OPERATION == "txn-put" ]]; then
                for TXN_OPS in 1 8 64; do
                    fill $TARGET $OPERATION $TXN_OPS
                done
            elif [[ $OPERATION == "txn-mixed" ]]; then
                for TXN_OPS in 1 8 64; do
                    fill $TARGET $OPERATION $TXN_OPS
                done
            fi
        done
    done
}

main
