import time
import multiprocessing as mp
import pandas as pd

csvfiles = ['folder/content1.csv','folder/content2.csv','folder/content3.csv','folder/content4.csv']

def readcsv(csvfile):
#	print('entered')
	df = pd.read_csv(csvfile)

start = time.time()
for csvfile in csvfiles:
#	print('entered')
	readcsv(csvfile)
end = time.time()
print(end-start)

p = mp.cpu_count()

st = time.time()
if __name__ == '__main__':
	jobs = []

#	st = time.time()
	for i in range(len(csvfiles)):
		p = mp.Process(target=readcsv,args=(csvfiles[i],))
		jobs.append(p)
		p.start()
#	en = time.time()-start
#	print(en-st)
	"""
	pool = mp.Pool(processes=2)
	s = time.time()
	for i in range(0,len(csvfiles)):
		k.append(pool.apply_async(readcsv,[csvfiles[i]]))
	pool.close()
	pool.join()
	e = time.time()-start
	print(e-s)
	"""
en = time.time()
print(en-st)
