#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR
# source ./.env

for i in "$@"; do
    case $i in
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

echo $(hostname): Generating TPCH-DB, scale = $SCALE

cd ../dbgen


if [ $CHUNKS -eq 1 ]
then
    ./dbgen -s $SCALE -f &> /dev/null &
else
    if [ $WORKER_ID -eq 0 ]
    then
        ./dbgen -S $n -s $SCALE -f -T l &> /dev/null &
    fi
    let start=$WORKER_ID+1
    for n in $(seq $start $WORKERS $CHUNKS); do 
        ./dbgen -C $CHUNKS -S $n -s $SCALE -f &> /dev/null &
    done
fi

wait

for table in customer lineitem nation orders partsupp part region supplier ; do
    mkdir -p ../data/SF-$SCALE/$table
    mv $table.tbl* ../data/SF-$SCALE/$table/
done
