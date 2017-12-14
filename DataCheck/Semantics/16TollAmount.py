from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader


def checktolls_amount (tolls_amount):   
    if (tolls_amount is None):
        return tolls_amount + ' ' + 'TEXT' + ' tolls_amount ' + 'NULL'
    else:
        try:
            v =float(tolls_amount)
            if (v >= 0):
                return tolls_amount + ' ' + 'Float' + ' tolls_amount ' + 'Valid'
            else:
                return tolls_amount + ' ' + 'Float' + ' tolls_amount ' + 'Invalid'
        except:
            return tolls_amount + ' ' + 'TEXT' + ' tolls_amount ' + 'NULL'

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
                c = b.map(lambda x: checktolls_amount(x[16]))
            else :
                c = b.map(lambda x: checktolls_amount(x[14]))
            z = z.union(c)
    c.saveAsTextFile("TollAmount.out")

