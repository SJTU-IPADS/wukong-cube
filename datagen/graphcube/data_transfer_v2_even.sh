src_ext="nt"
src_dir="hyper_data"
dst_dir="hyper_date_repartition"

function rePartition() {
    rm $dst_dir -r
    mkdir $dst_dir
    count=0

    for file in $(ls $src_dir | grep .$src_ext)
    do
        echo "export "$file" to all.txt"
        cat $src_dir/$file >> $dst_dir/all.txt
        count=$((count+1))
    done

    total=`cat $dst_dir/all.txt | wc -l`
    partition_sz=$((total / count + 1))
    echo $total","$count","$partition_sz

    split -l $partition_sz --additional-suffix=".nt" -d -a 1 $dst_dir/all.txt $dst_dir/hyper_id_uni
}

rePartition