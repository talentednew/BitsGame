from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader


def checkpassNum (passNum):
    if (passNum is None):
        return passNum + ' ' + 'TEXT' + ' passenger_count ' + 'NULL'
    else:
        try:
            v = int(passNum)
            if (v >= 1 and v <= 9):
                return passNum + ' ' + 'Int' + ' passenger_count ' + 'Valid'
            else:
                return passNum + ' ' + 'Int' + ' passenger_count ' + 'Invalid'
        except:
            return passNum + ' ' + 'TEXT' + ' passenger_count ' + 'NULL'

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
            c = b.map(lambda x: checkpassNum(x[3]))
            z = z.union(c)
    c.saveAsTextFile("PassCount.out")
