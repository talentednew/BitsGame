from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader


def checktripDis (tripDis):
    if (tripDis is None):
        return tripDis + ' ' + 'TEXT' + ' trip_distance ' + 'NULL'
    else:
        try:
            v = float(tripDis)
            if (v > 0):
                return tripDis + ' ' + 'Float' + ' trip_distance ' + 'Valid'
            else:
                return tripDis + ' ' + 'Float' + ' trip_distance ' + 'Invalid'
        except:
            return tripDis + ' ' + 'TEXT' + ' trip_distance ' + 'NULL'

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
            c = b.map(lambda x: checktripDis(x[4]))
            z = z.union(c)
    c.saveAsTextFile("TripDis.out")








