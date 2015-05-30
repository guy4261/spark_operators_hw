#!/usr/bin/env pyspark

# To toy with this implementaiton, use ipython with Spark:
# $ export PYSPARK_DRIVER_PYTHON=ipython
# To silence off the pyspark logger messages:
# http://stackoverflow.com/questions/25193488/how-to-turn-off-info-logging-in-pyspark

#################################
# Spark Submit boilerplate code #
#################################

import pyspark
from pyspark import SparkContext, SparkConf

appName = "test"
master = "local"

conf = SparkConf().setAppName(appName).setMaster(master)

sc = SparkContext(conf=conf)


################################################
# Try reading input file according to CLI args #
################################################

import sys
import os

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


############################################
# Kendall_tau_rank_correlation_coefficient #
############################################

def kendall(self):
	#alias, left from development
	example_rdd = self
	all_pairs = example_rdd.cartesian(example_rdd)

	def calc(pair):
		p1, p2 = pair
		x1, y1 = p1
		x2, y2 = p2
		if (x1 == x2) and (y1 == y2):
			return ("t", 1) #tie
		elif ((x1 > x2) and (y1 > y2)) or ((x1 < x2) and (y1 < y2)):
			return ("c", 1) #concordant pair
		else:
			return ("d", 1) #discordant pair

	results  = all_pairs.map(calc)
	from operator import add
	results = results.aggregateByKey(0, add, add)

	n  = example_rdd.count()
	d = {k: v for (k, v) in results.collect()}

	# http://en.wikipedia.org/wiki/Kendall_tau_rank_correlation_coefficient
	tau = (d["c"] - d["d"]) / (0.5 * n * (n-1))
	return tau


setattr(pyspark.rdd.RDD, "kendall", kendall)

if __name__ == "__main__":
	if len(sys.argv) == 1: # no CLI arguments
		example_rdd = generate_example_rdd()

	else:
		input_file = sys.argv[1]
		if not os.path.isfile(input_file):
			print "Input file %s does not exist, exiting..." % (input_file)
			exit(1)

		example_rdd = read_data_from_file(input_file)
	
	print "tau: %f" % (example_rdd.kendall())
