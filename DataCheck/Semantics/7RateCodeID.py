from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader


def checkRatecodeID (RatecodeID):
    if (RatecodeID is None):
        return RatecodeID + ' ' + 'TEXT' + ' RatecodeID ' + 'NULL'
    else:
        try:
            v = int(RatecodeID)
            if (v >= 1 and v <= 6):
                return RatecodeID + ' ' + 'Int' + ' RatecodeID ' + 'Valid'
            else:
                return RatecodeID + ' ' + 'Int' + ' RatecodeID ' + 'Invalid'
        except:
            return RatecodeID + ' ' + 'TEXT' + ' RatecodeID ' + 'NULL'

if __name__ == "__main__":
    sc = SparkContext()
    z = sc.parallelize([])
    for i in range(2015, 2017):
        for j in range(1, 13):
            m = "0" + `j`
            if (j > 9):
                m = `j`
            inpath = "/user/jw4468/bigDataProject/data/originals/yellow_tripdata_" + `i` + "-" + m +".csv"
            a = sc.textFile(inpath, 1)
            header = a.first()
            a = a.filter(lambda line: line != header)
            b = a.mapPartitions(lambda x: reader(x))
            if (i == 2015 or j < 7):
                c = b.map(lambda x: checkRatecodeID(x[7]))
            else :
                c = b.map(lambda x: checkRatecodeID(x[5]))
            z = z.union(c)
    c.saveAsTextFile("RatecodeID.out")
