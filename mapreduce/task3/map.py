#!/share/apps/python/3.6.5/bin/python
import sys
from csv import reader


for line in reader(sys.stdin):
	
	print('{0:s}\t{1:s}, {2:d}'.format(line[2], line[12], 1))