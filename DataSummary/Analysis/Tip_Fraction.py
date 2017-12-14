from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: tip fraction <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    lines = lines.filter(lambda x:x[11]=="1"or x[11]=="3" or x[11]=="4" or x[11]=="5" or x[11]=="6")
    counts = lines.map(lambda lines: (lines[1],float(lines[15].replace("tip_amount","0.0"))/float(lines[12].replace("fare_amount","0.0"))))
    counts = counts.aggregateByKey((0,0), lambda a,b:(a[0]+b, a[1]+1),lambda a,b: (a[0]+b[0], a[1]+b[1])) 

    counts.map(lambda x:"%s\t%.2f"%(x[0],x[1][0]/x[1][1])).saveAsTextFile("path.out")

    sc.stop()
