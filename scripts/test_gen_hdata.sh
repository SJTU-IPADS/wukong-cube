TARGET_DIR="hyper_data/"
DATA_FILE_PREFIX="hyper_id_uni"
DATA_FILE_SUFFIX=".nt"
STR_INDEX_FILE="str_index"
HPYER_STR_INDEX_FILE="hyper_str_index"
STR_NORMAL_FILE="str_normal"

DATA_FILE_NUM=5
DATA_FILE_LINE=3

# normal string example: <http://www.Department15.University3.edu/AssistantProfessor10>
# index string example: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#name>

# range
# tid: [2, 10]
# htid: [11, 20]
# vid: [(1 << 16), (1 << 16) + 19]
MIN_TID=2
MAX_TID=10
MIN_HTID=11
MAX_HTID=20
MIN_VID=$((1 << 16))
MAX_VID=$((MIN_VID + 19))

function clear_data {
    rm $TARGET_DIR -r
    mkdir $TARGET_DIR
}

# generate str_index
function gen_str_index() {
    echo -e "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t\t0" >> $TARGET_DIR$STR_INDEX_FILE
    
    # for each type
    for k in `seq $MIN_TID $MAX_TID`
    do
        echo -e "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type$k>\t\t$k" >> $TARGET_DIR$STR_INDEX_FILE
    done
}

# generate hyper_str_index
function gen_hyepr_str_index() {
    echo -e "<http://www.w3.org/1999/02/22-rdf-syntax-ns#hypertype>\t\t1" >> $TARGET_DIR$HPYER_STR_INDEX_FILE

    # for each hyper type
    for k in `seq $MIN_HTID $MAX_HTID`
    do
        echo -e "<http://www.w3.org/1999/02/22-rdf-syntax-ns#hypertype$k>\t\t$k" >> $TARGET_DIR$HPYER_STR_INDEX_FILE
    done
}

# generate str_normal
function gen_str_normal {
    # vid_base=$((1 << 16))

    # for each normal vertex
    for vid in `seq $MIN_VID $MAX_VID`
    do
        # vid=$((k+vid_base))
        echo -e "<http://www.DepartmentX.UniversityY.edu/Entity$vid>\t\t$vid" >> $TARGET_DIR$STR_NORMAL_FILE
    done
}

function gen_data {
    # generate str_normal
    vid_base=$((1 << 16))
    he_nums=0

    # for each data file
    for i in `seq 0 $(( DATA_FILE_NUM - 1))`
    do 
        echo "generating data file $i..."

        # generate each line in this data file
        for lnum in `seq 0 $((DATA_FILE_LINE - 1))`
        do
            line="<http://www.DepartmentX.UniversityY.edu/HyperEdge$he_nums>\t"
            he_nums=$((he_nums+1))
            # generate htid in this line
            htid=$(($RANDOM % 10 + MIN_HTID))
            line=$line$htid"\t|\t"
            # generate vids in this line
            for vnum in `seq 0 $(($RANDOM % 5 + 1))`
            do
                vid=$(($RANDOM % 20 + MIN_VID))
                line=$line$vid"\t"
            done
            line=$line"|\t1.0"
            echo -e $line >> $TARGET_DIR$DATA_FILE_PREFIX$i$DATA_FILE_SUFFIX
            echo -e $line >> $TARGET_DIR"hyper_data_all"
        done
    done
}

clear_data
gen_str_index
gen_hyepr_str_index
gen_str_normal
gen_data