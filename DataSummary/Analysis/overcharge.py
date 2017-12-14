from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: overcharge issue <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    lines = lines.filter(lambda lines: lines[9]=="1"and float(lines[18].replace("total_amount","0.0"))-float(lines[17].replace("improvement_surcharge","0.0"))-float(lines[16].replace("tolls_amount","0.0"))-float(lines[15].replace("tip_amount","0.0"))-float(lines[14].replace("mta_tax","0.0"))-float(lines[13].replace("extra","0.0"))-float(lines[12].replace("fare_amount","0.0"))>3.0)
    counts = lines.map(lambda lines: (lines[1].split(' ')[0],1)).reduceByKey(add)
    #m = sc.parallelize(counts)
    counts.map(lambda x:"%s\t%s"%(x[0],x[1])).saveAsTextFile("path.out")

    sc.stop()
