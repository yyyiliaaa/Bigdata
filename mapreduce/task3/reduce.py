#!/share/apps/python/3.6.5/bin/python
import sys


current_key = None

for line in sys.stdin:
	line = line.strip()
	key, value = line.split('\t')
	total, number = value.split(', ')
	
	if key == current_key:
		current_total += float(total)
		current_number += int(number)

	else:
		if current_key:
			print('{0:s}\t{1:.2f}, {2:.2f}'.format(current_key, current_total, (current_total/current_number)))
		current_key = key
		current_total = float(total)
		current_number = int(number)



print('{0:s}\t{1:.2f}, {2:.2f}'.format(current_key, current_total, (current_total/current_number)))
