#!/bin/bash

input_file="fraudTrain.csv"


# Create 5 result files with numbers as suffix
for i in {1..5}; do
    start_line=$((111144 * (i - 1) + 1))
    end_line=$((111144 * i))

    output_file="fraudTrain_${i}.csv"

    # Extract lines from input file and save to output file
    { head -n "$end_line" "$input_file"; } > "$output_file"

    echo "Created $output_file with lines $start_line to $end_line"
done
