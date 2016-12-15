for i in {0..11}
do
    echo "process $((12*($i))) $((12*($i+1))) $i"
    python create.py ../data/raw ../data/processed/json_output $((12*($i))) $((12*($i+1))) $i &    
done

# python create.py ../data/raw ../data/processed/json_output 0 1 0 &
# python create.py ../data/raw ../data/processed/json_output 1 2 1 &
# python create.py ../data/raw ../data/processed/json_output 2 3 2 &
# python create.py ../data/raw ../data/processed/json_output 3 4 3 &

# python create.py ../data/raw ../data/processed/json_output 4 5 4 &
# python create.py ../data/raw ../data/processed/json_output 5 6 5 &
# python create.py ../data/raw ../data/processed/json_output 6 7 6 &
# python create.py ../data/raw ../data/processed/json_output 7 8 7 &

# python create.py ../data/raw ../data/processed/json_output 8 9 8 &
# python create.py ../data/raw ../data/processed/json_output 9 10 9 &
# python create.py ../data/raw ../data/processed/json_output 10 11 10 &
# python create.py ../data/raw ../data/processed/json_output 11 12 11 &

# python create.py ../data/raw ../data/processed/json_output 12 13 12 &
# python create.py ../data/raw ../data/processed/json_output 13 14 13 &
# python create.py ../data/raw ../data/processed/json_output 14 15 14 &
# python create.py ../data/raw ../data/processed/json_output 15 16 15 &

wait 
echo "All done"