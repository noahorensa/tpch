DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR
source .env

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

        *)
        echo "Unknown argument $i"    # unknown option
        ;;
    esac
done

echo Parallel TPCH dbgen
echo Scale factor = $SCALE

#remove empty lines from workers
sed -i '/^$/d' workers

WORKERS=$(cat workers | wc -l)

i=0
for m in $(cat workers) ; do
    if [ $(pgrep -c -P$$) -ge $MAX_CONCURRENT_SSH ]; then
        wait -n
    fi

    ssh -A $(whoami)@$m "$TPCH_HOME/sbin/parallel_dbgen.sh -sf $SCALE -c $CHUNKS -w $WORKERS -wid $i"&

    let i=$i+1
done

wait
