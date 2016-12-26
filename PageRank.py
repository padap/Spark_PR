import numpy as np
import re
import sys
from operator import add
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession\
    .builder\
    .appName("PythonSparkPR")\
    .getOrCreate()
FILEIN = "test.txt"
ITERATIONNUMBER = 50


def part_of_PR(urls, rank):
#   new part of PageRank  
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)
        
def printRDD(rdd):
#   debugging function
    try:
        rdd.toDF().show()
    except:
        print("can't print")
        
# Load data in formar 
# id1\tid2
# id1    id3
# idn    idk
lines     = spark.read.text(FILEIN).rdd

# Build Matrix 
# _1/_2
# 1:[2,3,4]
# 2:[1,2]
# ...
matrix_urls = lines.map(lambda urls: re.split(r'\s+', urls[0])).groupByKey()
N = matrix_urls.count()

# build Rank
# _1/_2
# 1:1
# 2:1
# 3:1
# ...
ranks = matrix_urls.map(lambda matrix_urls : (matrix_urls[0], 1.0))

for iteration in xrange(ITERATIONNUMBER):
        ranks = matrix_urls\
        .join(ranks)\
        .flatMap(
            lambda (url, (urls, rank)): part_of_PR(urls, rank))\
        .reduceByKey(add)\
        .mapValues(
            lambda part_of_PR: part_of_PR * 0.85 + 0.15/N)

for (link, rank) in sorted(ranks.collect(), key = lambda x:int(x[0])):
    print("PR(%s) = %s." % (link, rank*1.0))
    
