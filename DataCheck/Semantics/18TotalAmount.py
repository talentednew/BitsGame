from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader


def checktotal_amount (total_amount):
    if (total_amount is None):
        return total_amount + ' ' + 'TEXT' + ' total_amount ' + 'NULL'
    else:
        try:
            v =float(total_amount)
            if (v > 0):
                return total_amount + ' ' + 'Float' + ' total_amount ' + 'Valid'
            else:
                return total_amount + ' ' + 'Float' + ' total_amount ' + 'Invalid'
        except:
            return total_amount + ' ' + 'TEXT' + ' total_amount ' + 'NULL'

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
                c = b.map(lambda x: checktotal_amount(x[18]))
            else :
                c = b.map(lambda x: checktotal_amount(x[16]))
            z = z.union(c)
    c.saveAsTextFile("TotalAmount.out")
