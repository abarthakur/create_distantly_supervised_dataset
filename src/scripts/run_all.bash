shopt -s globstar
for i in ../data/raw3/**/wiki*; do 
    out_file_path=../data/processed/json_output/${i:13:10}.json
    log_file_path=../data/log/${i:13:10}.log
    if [ ! -f $out_file_path ]; then
        echo "Processing $out_file_path"
        install /dev/null -D $out_file_path
        install /dev/null -D $log_file_path
        time python create_final.py $i $out_file_path >> $log_file_path
    fi

done

