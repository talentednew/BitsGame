from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from datetime import datetime
from csv import reader


def checkDOTime (DOtime, y, m):
    if (DOtime is None):
        return DOtime + ' ' + 'TEXT' + ' tpep_dropoff_datetime ' + 'NULL'
    else:
        try:
            v = datetime.strptime(DOtime, "%Y-%m-%d %H:%M:%S")
            if (DOtime == v.strftime("%Y-%m-%d %H:%M:%S")) and int(DOtime[:4]) == y and int(DOtime[5:7]) == m:
                return DOtime + ' ' + 'TimeStamp' + ' tpep_dropoff_datetime ' + 'Valid'
            else:
                return DOtime + ' ' + 'TimeStamp' + ' tpep_dropoff_datetime ' + 'Invalid'
        except ValueError:
            return DOtime + ' ' + 'TEXT' + ' tpep_dropoff_datetime ' + 'NULL'

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
            c = b.map(lambda x: checkDOTime(x[2], i, j))
            z = z.union(c)
    c.saveAsTextFile("DOtime.out")




