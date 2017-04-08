shopt -s globstar
for i in ../data/processed/json_output1/**/wiki*; do 
    out_file_path=../data/processed/json_output2/${i:31:10}.json
    log_file_path=../data/log2/${i:31:10}.log
    if [ ! -f $out_file_path ]; then
        echo "Processing $out_file_path"
#        echo "Processing $log_file_path"
        install /dev/null -D $out_file_path
        install /dev/null -D $log_file_path
        
        time python create_spotlight.py $i $out_file_path > $log_file_path
    fi

done

