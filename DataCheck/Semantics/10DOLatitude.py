from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader


def checkDOLat (DOLat):
    if (DOLat is None):
        return DOLat + ' ' + 'TEXT' + ' dropoff_latitude ' + 'NULL'
    else:
        try:
            v = float(DOLat)
            if (v >= 40.5774 and v <= 40.9176):
                return DOLat + ' ' + 'Float' + ' dropoff_latitude ' + 'Valid'
            else:
                return DOLat + ' ' + 'Float' + ' dropoff_latitude ' + 'Invalid'
        except:
            return DOLat + ' ' + 'TEXT' + ' dropoff_latitude ' + 'NULL'

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
                c = b.map(lambda x: checkDOLat(x[10]))
                z = z.union(c)
    c.saveAsTextFile("DOLat.out")

