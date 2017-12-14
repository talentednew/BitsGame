#!/usr/bin/env python

import sys
import string
import csv
import StringIO

for line in sys.stdin:
	csv_file = StringIO.StringIO(line)
	csv_reader = csv.reader(csv_file, delimiter=',')
	for values in csv_reader:
		trip_distance = values[4]
		mapOutputkey = str(trip_distance)
		print '%s\t%s' % (mapOutputkey, 1)

	


  