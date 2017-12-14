from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from datetime import datetime
from csv import reader


def checkPUTime (PUtime, y, m):
    if (PUtime is None):
        return PUtime + ' ' + 'TEXT' + ' tpep_pickup_datetime ' + 'NULL'
    else:
        try:
            v = datetime.strptime(PUtime, "%Y-%m-%d %H:%M:%S")
            if (PUtime == v.strftime("%Y-%m-%d %H:%M:%S")) and int(PUtime[:4]) == y and int(PUtime[5:7]) == m:
                return PUtime + ' ' + 'TimeStamp' + ' tpep_pickup_datetime ' + 'Valid'
            else:
                return PUtime + ' ' + 'TimeStamp' + ' tpep_pickup_datetime ' + 'Invalid'
        except ValueError:
            return PUtime + ' ' + 'TEXT' + ' tpep_pickup_datetime ' + 'NULL'

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
            c = b.map(lambda x: checkPUTime(x[1], i, j))
            z = z.union(c)
    c.saveAsTextFile("PUtime.out")

