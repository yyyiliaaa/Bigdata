#!/share/apps/python/3.6.5/bin/python

import os
import sys
from csv import reader

for line in reader(sys.stdin):
	file_name = os.environ['mapreduce_map_input_file']
	if 'parking' in file_name:
		print('{0:s}\t{1:s}, {2:s}, {3:s}, {4:s}'.format(line[0], line[14], line[6], line[2], line[1]))
	elif 'open' in file_name:
		print('{0:s}\t{1:s}'.format(line[0], 'open_violation'))