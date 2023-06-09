#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR
# source ./.env

while [[ $# -gt 0 ]]; do
    key="$1"

    case $key in
        -sf)
        SCALE=$2
        shift
        shift
        ;;

        -c)
        CHUNKS=$2
        shift
        shift
        ;;

        -w)
        WORKERS=$2
        shift
        shift
        ;;

        -wid)
        WORKER_ID=$2
        shift
        shift
        ;;

        *)
        echo "Unknown argument $i"    # unknown option
        ;;
    esac
done

echo $(hostname): Generating TPCH-DB, scale = $SCALE, chunks=$CHUNKS, worker=$WORKER_ID

cd ../dbgen

start_time=$(date +%s.%3N)

if [ $CHUNKS -eq 1 ]; then
    ./dbgen -s $SCALE -f &> /dev/null &
else
    if [ $WORKER_ID -eq 0 ]; then
        ./dbgen -s $SCALE -f -T l &> /dev/null &
    fi
    let start=$WORKER_ID+1
    for n in $(seq $start $WORKERS $CHUNKS); do 
        if [ $(pgrep -c -P$$) -ge $(nproc) ]; then
            wait -n
        fi
        ./dbgen -C $CHUNKS -S $n -s $SCALE -f -T o &> /dev/null &

        if [ $(pgrep -c -P$$) -ge $(nproc) ]; then
            wait -n
        fi
        ./dbgen -C $CHUNKS -S $n -s $SCALE -f -T p &> /dev/null &

        if [ $(pgrep -c -P$$) -ge $(nproc) ]; then
            wait -n
        fi
        ./dbgen -C $CHUNKS -S $n -s $SCALE -f -T s &> /dev/null &

        if [ $(pgrep -c -P$$) -ge $(nproc) ]; then
            wait -n
        fi
        ./dbgen -C $CHUNKS -S $n -s $SCALE -f -T S &> /dev/null &

        if [ $(pgrep -c -P$$) -ge $(nproc) ]; then
            wait -n
        fi
        ./dbgen -C $CHUNKS -S $n -s $SCALE -f -T c &> /dev/null &
    done
fi

wait

for table in customer lineitem nation orders partsupp part region supplier ; do
    if ls $table.tbl* 1> /dev/null 2>&1; then
        mkdir -p ../data/SF-$SCALE/$table
        mv $table.tbl* ../data/SF-$SCALE/$table/
    fi
done

end_time=$(date +%s.%3N)

elapsed=$(echo "scale=3; $end_time - $start_time" | bc)

echo $(hostname): worker=$WORKER_ID, elapsed_time = $elapsed
