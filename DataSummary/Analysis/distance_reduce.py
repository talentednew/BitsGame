#!/usr/bin/env python

import sys
import string

current_trip_distance = None
current_count_0to5 = 0
current_count_5to10 = 0
current_count_10to15 = 0
current_count_15to20 = 0
current_count_morethan20 = 0


for line in sys.stdin:

    tp, count = line.strip().split("\t", 1)
    

    try:
        count = int(count)
    except ValueError:
        continue


    if float(tp.replace("trip_distance","0.0")) <5 :
        current_count_0to5 += count
    elif float(tp.replace("trip_distance","0.0")) >=5 and float(tp.replace("trip_distance","0.0"))<10:
        current_count_5to10 += count
    elif float(tp.replace("trip_distance","0.0")) >=10 and float(tp.replace("trip_distance","0.0"))<15:
        current_count_10to15 += count
    elif float(tp.replace("trip_distance","0.0")) >=15 and float(tp.replace("trip_distance","0.0"))<20:
        current_count_15to20 += count
    else:
        current_count_morethan20 +=count   
        
            

print "%s\t%s" % ("0to5", current_count_0to5)
print "%s\t%s" % ("5to10", current_count_5to10)
print "%s\t%s" % ("10to15", current_count_10to15)
print "%s\t%s" % ("15to20", current_count_15to20)
print "%s\t%s" % ("morethan20", current_count_morethan20)
