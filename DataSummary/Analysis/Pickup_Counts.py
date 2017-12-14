from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: pickup locations mapping <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    counts = lines.map(lambda lines: (lines[5]+"\t"+lines[6],1)).reduceByKey(add)

    counts.map(lambda x:"%s\t%s"%(x[0],x[1])).saveAsTextFile("path.out")

    sc.stop()
