# Massive Data Mining - Homework 1 #
### Yasmin Bokobza 200701985
### Guy Rapaport 021527734


##################
## Introduction ##
##################

In this report we will review the operators we implemented in Spark.
However, some underlying principles were taken into account
across all algorithms.
Based on the list of transformations actions in the Spark programing guide:
https://spark.apache.org/docs/latest/programming-guide.html

List of transformations:
https://spark.apache.org/docs/latest/programming-guide.html#transformations

List of actions:
https://spark.apache.org/docs/latest/programming-guide.html#actions

We concluded that transformations are good, and actions are bad.

By good we mean lazy, and thus expected to be optimized 
by the framework. We are also expecting these optimizations to improve
in future releases.

By bad we mean eager, that is - causing a materialization, and by that
forcing the framework to consume resources etc.

Our aim was to use as many transformations as possible, and as little
actions as possible - narrowing our code to only a single action, if possible,
which will return the expected value of the parameter.

We did not use any "special" data structures - the most complicated thing used
was using lists of numbers rather than numbers in our RDD.

3 operators were implemented: ```median```, ```add_noise```, ```spearman```.


#################
## Driver Part ##
#################

Since Python supports dynamic addition of operators to existing classes, we
simply added the operators to the RDD class and used them directly:

```python
setattr(pyspark.rdd.RDD, "median", median)
```

We used command line arguments parsing (of ```sys.argv```) to determine the
CLI arguments and parse the input files, or simple created example data.


##########################
## How to run this code ##
##########################

Assuming ```pyspark``` is on the machine's PATH, simply execute run.sh to get
the example executions for every algorithm.

To use your own input, the command line arguments are:

```bash
$ ./pyspark_median.py {input file: list of numbers}
$ ./pyspark_add_noise.py {input file: list of numbers} {noise prob.} {magnitude}
$ ./pyspark_spearman.py {input file: tsv with two columns of numbers}
```
Example:

```bash
$ ./pyspark_median.py list_example_input.tsv
$ ./pyspark_add_noise.py list_example_input.tsv 0.3 5
$ ./pyspark_spearman.py correlation_example_input
```

The input for the median and noise operators is a list of numbers, each number
in a new line:

```
106
7
86
0
100
27
101
50
99
28
103
29
97
20
113
12
112
6
110
17
10
11
```

The input for the Kendall Tau and Spearman correleations is a list of 2 columns
of numbers, each pair in a new line, separated by tabs:

```
106	7
86	0
100	27
101	50
99	28
103	29
97	20
113	12
112	6
110	17
10	11
```


############
## Median ##
############

To get the median in an list stored in local memory, you simply sort it and
extract the middle element by its index (or the average of the two
middle elements in a list of even length).

But in distributed data you don't have random access and cannot simply
access the middle element(s) by index.

So we have to
- determine the length of the data (count)
- sort the data (sortBy)
- assign indices to the sorted elements (zipWithIndex)
- pick the element whose index is the one the median should have (lookup)

As we mentioned before, we also want to use only transformations, because
actions can take a lot of memory given large datasets.

This implementation uses the following RDD methods:

* count - action - returns single int regardless of the data size.

* sortBy - transformation - lambda-based version of sortByKey.

* zipWithIndex - transformation - Zips this RDD with its element indices.

* map - transofmration - you know it :)

* lookup - action - For an RDD of the form [(key, value) ... (key, value)] -
return the list of values in the RDD for key `key`.
Returns a 2-tuple regardless of the data size.

That means that our implementation will use the spark engine to its fullest
since all the heavy lifting is done by transformations and the only actions
which will store results on the local machine are count and lookup
which have a very small footprint.

This is the code:

```python
def median(self):

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
        required = rdd.filter(lambda (i, _): i == index1 or i == index2)
        a = required.lookup(index1)[0]
        b = required.lookup(index2)[0]
        
        median = ((a + b) / 2.0)
    
    return float(median)
```

Explanation:

* Line 3: Get the count of elements in the list.

* Line 5: Avoid unnecessary work and exit if list is empty.

* Line 8: Get the index of the median (if list is even special handling
will be done later).

* Line 10: The interesting part:

1. Sort the list - since the median is the "middle" element.
1. ZipWithIndex to get the enumeration for each item...
1.  and use map to switch the order so that the ordering index of each element
is the first item in the pair.

* Lines 13-14: if the list is odd, use the lookup() operator to find to
collect the middle element.

* Lines 16-24: if the list is even, filter only the middle elements,
and collect their average as the median.


###############
## Add Noise ##
###############

This is a very simple operator:

```python
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
```

The ```noise(x)``` function simply adds random noise of random magnitude in
[-m, m] with probability p to the element x.

The only thing left for Spark to do is to map the RDD elements using it!


############################
## Spearman's Correlation ##
############################

The implementation was done according to the Wikipedia article:
http://en.wikipedia.org/wiki/Spearman%27s_rank_correlation_coefficient

According to this implementation, one should:

    Sort the data by the first column. Create a new, third column with the ranked values 1,2,3,...n.
    
    Next, sort the data by the second column. Create a fourth column y_i and similarly assign it the ranked values 1,2,3,...n.
    
    Create a fifth column d_i to hold the differences between the two rank columns (x_i and y_i).
    
    Create one final column d^2_i to hold the value of column d_i squared.


We simply understood it as the simple rule:

	For a pair of values, get their ranks and compute d_i squared.

So to achieve this, we

1. Split the data into two seperate columns,

1. For each column, create an RDD with its values' ordering index. An ordering
index is the 0-based ordering of the values in the sorted list.

1. For each column, create another RDD with its values' ranking index. A ranking
index is the 1-based ordering of the sorted list. If two values are equal they
are assigned the mean of their ranks.

1. For each column, join its value-to-rank-index and value-to-ordering-index
RDDs by the value. Now you have, for each column, it's ordering-index-to-rank RDD.

1. Join the two RDDs by the ordering index. Now you have the
ordering-index-to-(rank1, rank2) RDD.

1. Compute d_i squared.


The rest of the computation is trivial. Our code reflects the above points 
quite explicitly so we avoid describing it here.


################
### Colophon ###
################

This document was written in markdown:
http://daringfireball.net/projects/markdown/

and compiled to PDF using ```gimli```:
https://github.com/walle/gimli

and sometimes using Remarkable:
http://remarkableapp.net/

It is available online on GitHub:
https://github.com/guy4261/spark_operators_hw

