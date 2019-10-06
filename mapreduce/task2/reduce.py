#!/share/apps/python/3.6.5/bin/python
import sys


current_key = None

for line in sys.stdin:
	line = line.strip()
	key, value = line.split('\t')
	
	if key == current_key:
		current_value += int(value)

	else:
		if current_key:
			print('{0:s}\t{1:d}'.format(current_key, current_value))
		current_key = key
		current_value = int(value)


print('{0:s}\t{1:d}'.format(current_key, current_value))
