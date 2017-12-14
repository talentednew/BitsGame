from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader


def checkPULocationID (PULocationID):
    if (PULocationID is None):
        return PULocationID + ' ' + 'TEXT' + ' PULocationID ' + 'NULL'
    else:
        try:
            v = int(PULocationID)
            if (v >= 1 and v <= 263):
                return PULocationID + ' ' + 'Int' + ' PULocationID ' + 'Valid'
            else:
                return PULocationID + ' ' + 'Int' + ' PULocationID ' + 'Invalid'
        except:
            return PULocationID + ' ' + 'TEXT' + ' PULocationID ' + 'NULL'

if __name__ == "__main__":
    sc = SparkContext()
    z = sc.parallelize([])
    for i in range(2016, 2017):
        for j in range(1, 13):
            if (i == 2016 and j >= 7):
                m = "0" + `j`
                if (j > 9):
                    m = `j`
                inpath = "/user/jw4468/bigDataProject/data/originals/yellow_tripdata_" + `i` + "-" + m +".csv"
                a = sc.textFile(inpath, 1)
                header = a.first()
                a = a.filter(lambda line: line != header)
                b = a.mapPartitions(lambda x: reader(x))
                c = b.map(lambda x: checkPULocationID(x[7]))
                z = z.union(c)
    c.saveAsTextFile("PULocationID.out")
