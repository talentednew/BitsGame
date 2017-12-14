from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: credit cash issue <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    lines = lines.filter(lambda x:x[9]=="1"or x[9]=="2")
    counts = lines.map(lambda lines: (lines[1].split(' ')[0]+"\t"+ lines[9],1)).reduceByKey(add).sortBy(lambda x:x[0], True)
    #m = sc.parallelize(counts)
    counts.map(lambda x:"%s\t%s"%(x[0],x[1])).saveAsTextFile("path.out")

    sc.stop()
