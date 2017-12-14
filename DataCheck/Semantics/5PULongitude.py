from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader


def checkPULong (PULong):
    if (PULong is None):
        return PULong + ' ' + 'TEXT' + ' pickup_longitude ' + 'NULL'
    else:
        try:
            v = float(PULong)
            if (v >= -74.15 and v <= -73.7004):
                return PULong + ' ' + 'Float' + ' pickup_longitude ' + 'Valid'
            else:
                return PULong + ' ' + 'Float' + ' pickup_longitude ' + 'Invalid'
        except:
            return PULong + ' ' + 'TEXT' + ' pickup_longitude ' + 'NULL'

if __name__ == "__main__":
    sc = SparkContext()
    z = sc.parallelize([])
    for i in range(2015, 2017):
        for j in range(1, 13):
            if (i == 2015 or j < 7):
                m = "0" + `j`
                if (j > 9):
                    m = `j`
                inpath = "/user/jw4468/bigDataProject/data/originals/yellow_tripdata_" + `i` + "-" + m +".csv"
                a = sc.textFile(inpath, 1)
                header = a.first()
                a = a.filter(lambda line: line != header)
                b = a.mapPartitions(lambda x: reader(x))
                c = b.map(lambda x: checkPULong(x[5]))
                z = z.union(c)
    c.saveAsTextFile("PULong.out")








