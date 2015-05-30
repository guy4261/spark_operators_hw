#!/usr/bin/env pyspark

# Median Operator - Implemented in PySpark
# Guy Rapaport and Yasmin Bokobza ({guyrap,yasminbo}@post.bgu.ac.il)
# Massive Data Mining Course, Autumn 2015
# Deptartment of Information Systems Engineering
# Ben-Gurion University of the Negev


# To toy with this implementaiton, use ipython with Spark:
# $ export PYSPARK_DRIVER_PYTHON=ipython

#################################
# Spark Submit boilerplate code #
#################################

import pyspark
from pyspark import SparkContext, SparkConf

appName = "test"
master = "local"

conf = SparkConf().setAppName(appName).setMaster(master)

sc = SparkContext(conf=conf)


#########################
# Genenate Example Data #
#########################

def generate_example_rdd():
	# Using data from: http://en.wikipedia.org/wiki/Spearman%27s_rank_correlation_coefficient#Example
	iqs = [106, 86, 100, 101, 99, 103, 97, 113, 112, 110]
	tv = [7, 0, 27, 50, 28, 29, 20, 12, 6, 17]
	assert len(iqs) == len(tv)
	example_data = zip(iqs, tv)
	example_rdd = sc.parallelize(example_data)
	return example_rdd

def check_row(row):
	row = row.strip("\r\n\t ")
	return len(row) > 0 and len(row.split("\t")) == 2

def read_data_from_file(filename):
	try:
		rdd = sc.textFile(input_file, use_unicode=False)
		rdd = rdd.filter(check_row)
		example_rdd = rdd.map(lambda row:map(float, row.split("\t")))
		return example_rdd

	except:
		print "Error reading data from tsv input file: %s" % (filename)
		print "Exiting..."
		exit(1)


###########################################
# Spearman's rank correlation coefficient #
###########################################

def spearman(self):

	# alias left from development
	example_rdd = self

	def split_pair(rdd):
		xs = rdd.map(lambda p:p[0])
		ys = rdd.map(lambda p:p[1])
		return xs, ys

	def mean(lst):
		if len(lst) == 0:
			return None
		else:
			return sum(lst) / float(len(lst))

	def get_index_to_rank(rdd):
		rdd_val_to_rank = rdd.sortBy(lambda x:x).zipWithIndex().map(
			lambda (x, i):(x, i+1)).groupByKey().map(
				lambda (k, iter_v):(k, mean(iter_v)))
		
		rdd_val_to_index = rdd.zipWithIndex()
		#rdd_val_to_index.collect()
		
		rdd_val_to_index_and_rank = rdd_val_to_index.join(rdd_val_to_rank)
		#rdd_val_to_index_and_rank.collect()
		
		rdd_index_to_rank = rdd_val_to_index_and_rank.map(lambda (v, i_r): i_r)
		#rdd_index_to_rank.collect()
		
		return rdd_index_to_rank


	xs, ys = split_pair(example_rdd)

	xs_index_to_rank = get_index_to_rank(xs)
	ys_index_to_rank = get_index_to_rank(ys)

	index_to_rankx_and_ranky = xs_index_to_rank.join(ys_index_to_rank)
	d2s = index_to_rankx_and_ranky.map(lambda (index, (rankx, ranky)): (rankx-ranky)**2)

	sum_d2s = d2s.sum()
	N = example_rdd.count()

	roh = 1 - ((6.0 * sum_d2s) / (N * (N**2 - 1)))

	return roh

# Dynamically add new operator to RDD class
setattr(pyspark.rdd.RDD, "spearman", spearman)


################################################
# Try reading input file according to CLI args #
################################################

import sys
import os

if __name__ == "__main__":
	if len(sys.argv) == 1: # no CLI arguments
		example_rdd = generate_example_rdd()

	else:
		input_file = sys.argv[1]
		if not os.path.isfile(input_file):
			print "Input file %s does not exist, exiting..." % (input_file)
			exit(1)

		example_rdd = read_data_from_file(input_file)

	# expecting, according to wikipedia: −0.175757575
	print "Spearman's coefficient value: %f" % (example_rdd.spearman())
