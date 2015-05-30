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


###################
# Median Operator #
###################

def median(self):
    """Find the median in a given dataset.

    To get the median in an locally-stored list, you simply sort it and
    extract the middle element (by its index - ~half of the length).
    
    But in distributed data you don't have random access and cannot simply
    access the middle element.
    
    So we have to
        - sort the data,
        - assign indices to the sorted elements,
        - pick the element whose index is the one the median should have.
    
    We also want to use only transformations, because actions can take
    a lot of memory given large datasets.
    
    List of transformations:
    https://spark.apache.org/docs/latest/programming-guide.html#transformations
    
    List of actions:
    https://spark.apache.org/docs/latest/programming-guide.html#actions
    
    This implementation uses the following RDD methods:
    
    * count - action - returns single int regardless of the data size.

    * sortBy - transformation - lambda-based version of sortByKey.

    * zipWithIndex - transformation - Zips this RDD with its element indices.

>>> sc.parallelize(["a", "b", "c", "d"], 3).zipWithIndex().collect()
[('a', 0), ('b', 1), ('c', 2), ('d', 3)]

    * map - transofmration - you know it :)

    * lookup - action - For an RDD of the form [(key, value) ... (key, value)] -
        return the list of values in the RDD for key `key`.
        Returns a 2-tuple regardless of the data size.
>>> sc.parallelize(["a", "b", "c", "d"], 3).zipWithIndex().collect()
[('a', 0), ('b', 1), ('c', 2), ('d', 3)]
        
    That means that our implementation will use the spark engine to its fullest
    since all the heavy lifting is done by transformations and the only actions
    which will store results on the local machine are count and lookup
    which have a very small footprint.
    """
       
    l = self.count()
    
    if l == 0:
        return None
    
    median_index = l / 2

    rdd = self.sortBy(lambda x:x).zipWithIndex().map(lambda(x,y):(y, x))
    
    
    if (l % 2) == 1:
       median = rdd.lookup(median_index)[0]
    
    if (l % 2) == 0:
        index1 = median_index - 1
        index2 = median_index
        required = rdd.filter(lambda (index, payload): index == index1 or index == index2)
        a = required.lookup(index1)[0]
        b = required.lookup(index2)[0]
        
        median = ((a + b) / 2.0)
    
    return float(median)


# Dynamically add new operator to RDD class
setattr(pyspark.rdd.RDD, "median", median)


##############
# Validation #
##############

try:
    from numpy import median as validation_median
    print "Successfully imported median function from numpy; using for validation."
except:
    def validation_median(lst):
        # Stolen shamelessly from
        # http://stackoverflow.com/a/24101655
        lst = sorted(lst)
        if len(lst) < 1:
                return None
        if len(lst) %2 == 1:
                return lst[((len(lst)+1)/2)-1]
        else:
                return float(sum(lst[(len(lst)/2)-1:(len(lst)/2)+1]))/2.0
    print "Failed importing median function from numpy; using version written in python."

############
# Examples #
############

if __name__ == "__main__":
    # Data preparation
    empty_lst = []

    odd_lst = [1,2,3,4,5]

    even_lst = [1,2,3,4,5,6]

    from random import randint
    MIN = 1
    MAX = 100
    N = 1000
    big_lst = [randint(MIN, MAX) for _ in xrange(N)]


    # Testing: expected vs. actual
    lsts =  {"empty list": empty_lst,
            "odd list": odd_lst,
            "even list": even_lst,
            "big list": big_lst}

    data_to_expected = {label: validation_median(lst)
                            for label, lst in lsts.iteritems()}
    data_to_actual   = {label: sc.parallelize(lst).median()
                            for label, lst in lsts.iteritems()}


    # Print out test results
    order = ["empty list", "odd list", "even list", "big list"]
    for label in order:
        actual = data_to_actual[label]
        expected = data_to_expected[label]
        print "Test:", label, ",expected:", expected, ". actual:", actual # , ", success:", expected == actual
        

