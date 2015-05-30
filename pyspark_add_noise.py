#!/usr/bin/env pyspark

# Add_Noise Operator - Implemented in PySpark
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


######################
# Add_Noise Operator #
######################

from random import random
def add_noise(self, p, m):
    """Map every element in the rdd to element+m with probability p 
    or leave it unchanged with probability (1-p)."""
    
    def noise(x):
        if random() < p:
            sign = (1 if random() < 0.5 else -1)
            noise = random() * m * sign
            x += noise
        return x

    return self.map(noise)

# Dynamically add new operator to RDD class
setattr(pyspark.rdd.RDD, "add_noise", add_noise)


############
# Examples #
############

def examples():
    # Data preparation
    empty_lst = []

    odd_lst = [1,2,3,4,5]

    even_lst = [1,2,3,4,5,6]

    from random import randint
    MIN = 1
    MAX = 100
    N = 20
    big_lst = [randint(MIN, MAX) for _ in xrange(N)]


    # Testing: expected vs. actual
    lsts =  {"empty list": empty_lst,
            "odd list": odd_lst,
            "even list": even_lst,
            "big list": big_lst}
    
    # Print out test results
    order = ["empty list", "odd list", "even list", "big list"]
    actual = {label: sc.parallelize(lsts[label]).add_noise(0.3, 5).collect() for label in order}
    
    from itertools import izip
    for label in order:
        original = lsts[label]
        noisey = actual[label]
        print label
        print original
        print noisey
        print


################################################
# Try reading input file according to CLI args #
################################################

import sys
import os
from itertools import izip

if __name__ == "__main__":

    if len(sys.argv) == 1:
        examples()

    else:
        try:
            filename = sys.argv[1]
            p = float(sys.argv[2])
            m = float(sys.argv[3])
            rdd = sc.textFile(filename, use_unicode=True)
            rdd = rdd.map(float)
            noisey_rdd = rdd.add_noise(p, m)
            print "Added noise:"
            for a, b in izip(rdd.collect(), noisey_rdd.collect()):
                tag = "=" if a == b else "!"
                print tag, a, "==>", b
        except:
            print "Error reading inout file! Exiting..."
            exit(1)

