shopt -s globstar
for i in ../data/raw4/**/wiki*; do 
    out_file_path=../data/processed/json_output1/${i:13:10}.json
    log_file_path=../data/log1/${i:13:10}.log
    if [ ! -f $out_file_path ]; then
        echo "Processing $out_file_path"
#        echo "Processing $log_file_path"
        install /dev/null -D $out_file_path
        install /dev/null -D $log_file_path
        
        time python create_dbpedia.py $i $out_file_path >> $log_file_path
    fi

done

