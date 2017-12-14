from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader


def checkVendorID (VendorID): 
    if (VendorID is None):
        return VendorID + ' ' + 'TEXT' + ' VendorID ' + 'NULL'
    else:
        try:
            v = int(VendorID)
            if (v == 1 or v == 2):
                return VendorID + ' ' + 'Int' + ' VendorID ' + 'Valid'
            else:
                return VendorID + ' ' + 'Int' + ' VendorID ' + 'Invalid'
        except:
            return VendorID + ' ' + 'TEXT' + ' VendorID ' + 'NULL'

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
            c = b.map(lambda x: checkVendorID(x[0]))
            z = z.union(c)
    c.saveAsTextFile("VendorID.out")
