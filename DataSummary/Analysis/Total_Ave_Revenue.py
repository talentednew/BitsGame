from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: fare and counts by time <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    #lines = lines.filter(lambda x:float(x[18]))
    counts = lines.map(lambda lines: (lines[1].split(' ')[0],float(lines[18].replace("total_amount","0.0"))))
    counts = counts.aggregateByKey((0,0), lambda a,b:(a[0]+b, a[1]+1),lambda a,b: (a[0]+b[0], a[1]+b[1])) 

    counts.map(lambda x:"%s\t%.2f\t%.2f"%(x[0],x[1][0],x[1][0]/x[1][1])).saveAsTextFile("path.out")

    sc.stop()
