#!/bin/bash

# Check if pyspark is available http://stackoverflow.com/a/677212
SPARK_EXECUTOR=pyspark
hash ${SPARK_EXECUTOR} 2>/dev/null || { echo >&2 "I require ${SPARK_EXECUTOR} but it's not installed.  Aborting."; exit 1; }

${SPARK_EXECUTOR} pyspark_median.py list_example_input.txt
${SPARK_EXECUTOR} pyspark_add_noise.py list_example_input.txt 0.3 5
${SPARK_EXECUTOR} pyspark_spearman.py correlation_example_input.tsv
