from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader


def checkISCharge (ISCharge):
    if (ISCharge is None):
        return ISCharge + ' ' + 'TEXT' + ' improvement_surcharge ' + 'NULL'
    else:
        try:
            v =float(ISCharge >= 0.299 and ISCharge <= 0.301)
            if (v > 0):
                return ISCharge + ' ' + 'Float' + ' improvement_surcharge ' + 'Valid'
            else:
                return ISCharge + ' ' + 'Float' + ' improvement_surcharge ' + 'Invalid'
        except:
            return ISCharge + ' ' + 'TEXT' + ' improvement_surcharge ' + 'NULL'

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
                c = b.map(lambda x: checkISCharge(x[17]))
            else :
                c = b.map(lambda x: checkISCharge(x[15]))
            z = z.union(c)
    c.saveAsTextFile("ISCharge.out")




