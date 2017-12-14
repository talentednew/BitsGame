from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader


def checkpayment_type (payment_type): 
    if (payment_type is None):
        return payment_type + ' ' + 'TEXT' + ' payment_type ' + 'NULL'
    else:
        try:
            v = int(payment_type)
            if (v >= 1 and v <= 6):
                return payment_type + ' ' + 'Int' + ' payment_type ' + 'Valid'
            else:
                return payment_type + ' ' + 'Int' + ' payment_type ' + 'Invalid'
        except:
            return payment_type + ' ' + 'TEXT' + ' payment_type ' + 'NULL'

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
                c = b.map(lambda x: checkpayment_type(x[11]))
            else :
                c = b.map(lambda x: checkpayment_type(x[9]))
            z = z.union(c)
    c.saveAsTextFile("paymentType.out")
