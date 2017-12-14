from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader


def checkDOLocationID (DOLocationID):
    if (DOLocationID is None):
        return DOLocationID + ' ' + 'TEXT' + ' DOLocationID ' + 'NULL'
    else:
        try:
            v = int(DOLocationID)
            if (v >= 1 and v <= 263):
                return DOLocationID + ' ' + 'Int' + ' DOLocationID ' + 'Valid'
            else:
                return DOLocationID + ' ' + 'Int' + ' DOLocationID ' + 'Invalid'
        except:
            return DOLocationID + ' ' + 'TEXT' + ' DOLocationID ' + 'NULL'

if __name__ == "__main__":
    sc = SparkContext()
    z = sc.parallelize([])
    for i in range(2016, 2017):
        for j in range(1, 13):
            if (j >= 7):
                m = "0" + `j`
                if (j > 9):
                    m = `j`
                inpath = "/user/jw4468/bigDataProject/data/originals/yellow_tripdata_" + `i` + "-" + m +".csv"
                a = sc.textFile(inpath, 1)
                header = a.first()
                a = a.filter(lambda line: line != header)
                b = a.mapPartitions(lambda x: reader(x))
                c = b.map(lambda x: checkDOLocationID(x[8]))
                z = z.union(c)
    c.saveAsTextFile("DOLocationID.out")

