from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader


def checkmta_tax (mta_tax):  
    if (mta_tax is None):
        return mta_tax + ' ' + 'TEXT' + ' mta_tax ' + 'NULL'
    else:
        try:
            v =float(mta_tax)
            if (v >= 0.5):
                return mta_tax + ' ' + 'Float' + ' mta_tax ' + 'Valid'
            else:
                return mta_tax + ' ' + 'Float' + ' mta_tax ' + 'Invalid'
        except:
            return mta_tax + ' ' + 'TEXT' + ' mta_tax ' + 'NULL'

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
                c = b.map(lambda x: checkmta_tax(x[14]))
            else :
                c = b.map(lambda x: checkmta_tax(x[12]))
            z = z.union(c)
    c.saveAsTextFile("MTATax.out")


