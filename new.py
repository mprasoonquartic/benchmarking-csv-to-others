import pandas as pd
import time
from multiprocessing import Process,current_process,Array,Value

csvfiles = ['folder/hydraulic1.csv','folder/hydraulic2.csv','folder/hydraulic3.csv','folder/hydraulic4.csv','folder/hydraulic5.csv']

def readcsv(csvfile,maxtime):
	b = time.time()
	pd.read_csv(csvfile)
	print(time.time()-b)
	

# Without using multiprocessing
"""
start = time.time()
for csvfile in csvfiles:
	readcsv(csvfile)

print(time.time()-start)
"""

# With using multiprocessing
if __name__ == '__main__':
	processes = []

	maxtime=Value('f',0.000000)
	
	for csvfile in csvfiles:
		
		process = Process(target=readcsv,args=(csvfile,maxtime))
		processes.append(process)

		process.start()
	
#		print('Final time is %f' % maxtime.value)
