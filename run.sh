#!/bin/bash

# Check if pyspark is available http://stackoverflow.com/a/677212
SPARK_EXECUTOR=pyspark
hash ${SPARK_EXECUTOR} 2>/dev/null || { echo >&2 "I require ${SPARK_EXECUTOR} but it's not installed.  Aborting."; exit 1; }

${SPARK_EXECUTOR} pyspark_median.py
${SPARK_EXECUTOR} pyspark_add_noise.py
${SPARK_EXECUTOR} pyspark_spearman.py
