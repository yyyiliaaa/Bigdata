#!/share/apps/python/3.6.5/bin/python
import sys


current_key = None

for line in sys.stdin:
	line = line.strip()
	key, value = line.split('\t')
	
	if key == current_key:
		current_value += value

	else:
		if current_key:
			if 'open_violation' not in current_value:
				print('{0:s}\t{1:s}'.format(current_key, current_value))
		
		current_key = key
		current_value = value

if 'open_violation' not in current_value:
	print('{0:s}\t{1:s}'.format(current_key, current_value))
