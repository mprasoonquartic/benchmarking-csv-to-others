import sqlite3
import pandas as pd
import numpy as np
import time
import pickle
import pyarrow.parquet as pq
import pyarrow as pa
import _pickle as cPickle
import feather
import os
from multiprocessing import Process,current_process

# Load the csv for current processing
# File size is 469MB

csvfile = 'hydraulic_systems_demo_data.csv'
df = pd.read_csv(csvfile)

# Time to read csv from pandas

def calculate_read_csv_time(csvfile):
    start = time.time()
    df = pd.read_csv(csvfile)
    csvreadtime = time.time()-start
    return csvreadtime

# Time to read csv from pandas in chunks
# Time to read csv in chunks is more than without chunks always

def calculate_read_csv_in_chunks_time(csvfile,csize):
    start = time.time()
    tempDF = pd.read_csv(csvfile,nrows=1)
    
    cols = tempDF.columns.values.tolist()
    newDF = pd.DataFrame(columns=cols)
    for chunk in pd.read_csv(csvfile,chunksize=csize):
        newDF = pd.concat([newDF,chunk])
    csvchunkreadtime = time.time()-start
    return csvchunkreadtime

# Time to read multiple csvs
# The file size is around 140MB,140MB,140MB,35MB,35MB

csvfiles = ['folder/hydraulic1.csv','folder/hydraulic2.csv','folder/hydraulic3.csv','folder/hydraulic4.csv','folder/hydraulic5.csv']

def calculate_multiple_csv_read_time(csvfiles):
    start = time.time()
    tempDF = pd.read_csv(csvfiles[0],nrows=1)
    
    cols = tempDF.columns.values.tolist()
    newdf = pd.DataFrame(columns=cols)
    for csvfile in csvfiles:
        localdf = pd.read_csv(csvfile)
        newdf = pd.concat([newdf,localdf])
    multiplecsvreadtime = time.time()-start
    return multiplecsvreadtime

# Saving in the sqlite db

sqlfile = 'hydraulic.sqlite3'

def calculate_df_to_sql_save_time(csvfile,sqlfile,nameoftable):
    sqlconnection = sqlite3.connect(sqlfile)
    start = time.time()
#   iteration=0
    sqlquery = "DELETE FROM %s" % nameoftable
    sqlcursor = sqlconnection.cursor()
    sqlcursor.execute(sqlquery)
    sqlcursor.fetchall()
    for chunk in pd.read_csv(csvfile,chunksize=1000):
        chunk.to_sql(name=nameoftable,con=sqlconnection,if_exists="append",index=False)
    sqlsavetime = time.time()-start
    return sqlsavetime

# Time to query in the sqlite db
# We are querying for the row where the timestamp is '2020-12-11 03:31:30'

def calculate_sql_query_time(sqlfile,nameoftable):
    sqlstatement = "SELECT * FROM %s WHERE timestamp IS '2020-12-11 03:31:30'" % nameoftable
    start = time.time()
    sqlconnection = sqlite3.connect(sqlfile)
    sqlcursor = sqlconnection.cursor()
    sqlcursor.execute(sqlstatement)
    sqlcursor.fetchall()
    sqlquerytime = time.time()-start
    return sqlquerytime

# Time to read the sql db

dfsql = None

def calculate_sql_to_df_load_time(sqlfile,dfsql,nameoftable):
    query = "SELECT * FROM %s" % (nameoftable)
    start = time.time()
    sqlconnection = sqlite3.connect(sqlfile)
    dfsql = pd.read_sql_query(query,sqlconnection)
    sqlreadtime = time.time()-start
    return sqlreadtime

# Time to save the pickle file

picklfile = "hydraulic.pkl"

def calculate_df_to_pickle_time(picklfile,df):
    start = time.time()
    with open(picklfile,"wb") as f:
        pickle.dump(df,f)
    picklesavetime = time.time()-start
    return picklesavetime

# Time to load the pickle file content as dataframe in pandas

dfpkl = None

def calculate_pickl_to_df_load_time(picklfile,dfpkl):
    start = time.time()
    with open(picklfile,"rb") as f:
        dfpkl = pickle.load(f)
    picklereadtime = time.time()-start
    return picklereadtime

# Time to save the dataframe as a parquet file
# Approach 1 - without using pytables

parquet_file1 = 'df.parquet.gzip'
compression_type = 'gzip'

def calculate_df_to_parquet_save_time_approach1(parquet_file,compression_type,df):
    start = time.time()
    df.to_parquet(parquet_file,compression=compression_type)
    parquetsavetime = time.time()-start
    return parquetsavetime

# Time to read the parquet file to the dataframe
# Approach 1

dfparquet1 = None

def calculate_parquet_to_df_load_time_approach1(parquet_file,dfparquet):
    start = time.time()
    dfparquet = pd.read_parquet(parquet_file)
    parquetloadtime = time.time()-start
    return parquetloadtime

# Time to save the parquet file to the dataframe
# Approach 2 - using pytables

parquet_file2 = 'df.parquet'

def calculate_df_to_parquet_save_time_approach2(parquet_file,df):
    start = time.time()
    table = pa.Table.from_pandas(df)
    pq.write_table(table,parquet_file)
    parquetsavetime = time.time()-start
    return parquetsavetime

# Time to read the parquet file to the dataframe
# Approach 2 - using pytables

dfparquet2 = None

def calculate_parquet_to_df_load_time_approach2(parquet_file,dfparquet):
    start = time.time()
    table = pq.read_table(parquet_file)
    dfparquet = table.to_pandas()
    parquetloadtime = time.time()-start
    return parquetloadtime

# Time to save cPickle file

cpklfile = 'hydraulicc.pkl'

def calculate_df_to_cpkl_save_time(cpklfile,df):
    start = time.time()
    with open(cpklfile,"wb") as f:
        cPickle.dump(df,f)
    cpicklesavetime = (time.time()-start)
    return cpicklesavetime

# Time to load cPickle file

dfcpkl = None

def calculate_cpkl_to_df_load_time(cpklfile,dfcpkl):
    start = time.time()
    with open(cpklfile,"rb") as f:
        dfcpkl = cPickle.load(f)
    cpickleloadtime = time.time()-start
    return cpickleloadtime

# Time to save dataframe as feather

featherfile = 'hydraulic.feather'

def calculate_df_to_feather_save_time(featherfile,df):
    os.remove(featherfile)
    start = time.time()
    feather.write_dataframe(df,featherfile)
    feathersavetime = time.time()-start
    return feathersavetime

# Time to read the feather file

dffeather = None

def calculate_feather_to_df_load_time(featherfile,dffeather):
    start = time.time()
    dffeather = feather.read_dataframe(featherfile)
    featherloadtime = time.time()-start
    return featherloadtime

# Time to query the dataframe
# Say we are quering to find the row with timestamp 2020-12-11 03:31:30

def calculate_time_to_query_df(df):
    start = time.time()
    df.loc[df['timestamp'] == "2020-12-11 03:31:30"]
    dfquerytime = time.time()-start
    return dfquerytime

n = 25
read_csv_times = []
read_multiple_csv_times = []
read_csv_chunks_times = []
df_to_sql_times = []
query_sql_times = []
sql_to_df_times = []
df_to_pkl_times = []
pkl_to_df_times = []
df_to_parquet_app1_times = []
parquet_to_df_app1_times = []
df_to_parquet_app2_times = []
parquet_to_df_app2_times = []
df_to_cpkl_times = []
cpkl_to_df_times = []
df_to_feather_times = []
feather_to_df_times = []
query_df_times = []

# Calculating average time calculations

for i in range(0,n):
    read_csv_time = calculate_read_csv_time(csvfile)
    read_csv_times.append(read_csv_time)
    
    print("CSV read times: %d - value: %f" % (i+1, read_csv_time))
    
    read_csv_chunks_time = calculate_read_csv_in_chunks_time(csvfile,1000000)
    read_csv_chunks_times.append(read_csv_chunks_time)
    
    print("CSV read times in chunks times: %d - value: %f" % (i+1,read_csv_chunks_time))
    
    read_multiple_csv_time = calculate_multiple_csv_read_time(csvfiles)
    read_multiple_csv_times.append(read_multiple_csv_time)
    
    print("Multiple CSV read times: %d - value: %f" % (i+1,read_multiple_csv_time))
    
    df_to_sql_time = calculate_df_to_sql_save_time(csvfile,sqlfile,'content')
    df_to_sql_times.append(df_to_sql_time)
    
    print("DF to SQL times: %d - value: %f" % (i+1,df_to_sql_time))
    
    query_sql_time = calculate_sql_query_time(sqlfile,'content')
    query_sql_times.append(query_sql_time)
    
    print("SQL Query times: %d - value: %f" % (i+1,query_sql_time))
    
    sql_to_df_time = calculate_sql_to_df_load_time(sqlfile,dfsql,'content')
    sql_to_df_times.append(sql_to_df_time)
    
    print("SQL to DF times: %d - value: %f" % (i+1,sql_to_df_time))
    
    df_to_pkl_time = calculate_df_to_pickle_time(picklfile,df)
    df_to_pkl_times.append(df_to_pkl_time)
    
    print("DF to Pickle times: %d - value: %f" % (i+1,df_to_pkl_time))
    
    pkl_to_df_time = calculate_pickl_to_df_load_time(picklfile,dfpkl)
    pkl_to_df_times.append(pkl_to_df_time)
    
    print("Pickle to DF times: %d - value: %f" % (i+1,pkl_to_df_time))
    
    df_to_parquet_app1_time = calculate_df_to_parquet_save_time_approach1(parquet_file1,compression_type,df)
    df_to_parquet_app1_times.append(df_to_parquet_app1_time)
    
    print("DF to Parquet approach 1 times: %d - value: %f" % (i+1,df_to_parquet_app1_time))
    
    parquet_to_df_app1_time = calculate_parquet_to_df_load_time_approach1(parquet_file1,dfparquet1)
    parquet_to_df_app1_times.append(parquet_to_df_app1_time)
    
    print("Parquet to DF approach 1 times: %d - value: %f" % (i+1,parquet_to_df_app1_time))
    
    df_to_parquet_app2_time = calculate_df_to_parquet_save_time_approach2(parquet_file2,df)
    df_to_parquet_app2_times.append(df_to_parquet_app2_time)
    
    print("DF to Parquet approach 2 times: %d - value: %f" % (i+1,df_to_parquet_app2_time))
    
    parquet_to_df_app2_time = calculate_parquet_to_df_load_time_approach2(parquet_file2,dfparquet2)
    parquet_to_df_app2_times.append(parquet_to_df_app2_time)
    
    print("Parquet to DF approach 2 times: %d - value: %f" % (i+1,parquet_to_df_app2_time))
    
    df_to_cpkl_time = calculate_df_to_cpkl_save_time(cpklfile,df)
    df_to_cpkl_times.append(df_to_cpkl_time)
    
    print("DF to cPickle times: %d - value: %f" % (i+1,df_to_cpkl_time))
    
    cpkl_to_df_time = calculate_cpkl_to_df_load_time(cpklfile,dfcpkl)
    cpkl_to_df_times.append(cpkl_to_df_time)
    
    print("cPickle to DF times: %d - value: %f" % (i+1,cpkl_to_df_time))
    
    df_to_feather_time = calculate_df_to_feather_save_time(featherfile,df)
    df_to_feather_times.append(df_to_feather_time)
    
    print("DF to feather times: %d - value: %f" % (i+1,df_to_feather_time))
    
    feather_to_df_time = calculate_feather_to_df_load_time(featherfile,dffeather)
    feather_to_df_times.append(feather_to_df_time)
    
    print("Feather to DF times: %d - value: %f" % (i+1,feather_to_df_time))
    
    query_df_time = calculate_time_to_query_df(df)
    query_df_times.append(query_df_time)
    
    print("Query DF times: %d - value: %f" % (i+1,query_df_time))

def calcavg(lst):
    total = 0
    for val in lst:
        total = total + val
    return total/len(lst)

def getmax(lst):
    mx=0
    for val in lst:
        if mx<val:
            mx=val
    return mx

def getmn(lst):
    mn=1000000
    for val in lst:
        if mn>val:
            mn=val
    return mn

print("The times are as follows:")
readcsvdetails = """Using pandas pd.read_csv:
Average read time : %f
Maximum read time : %f
Minimum read time : %f
""" % (calcavg(read_csv_times),getmax(read_csv_times),getmn(read_csv_times))
print(readcsvdetails)

print("Reading csv in chunks")
readcsvinchunks = """
Average read time : %f
Maximum read time : %f
Minimum read time : %f
""" % (calcavg(read_csv_chunks_times),getmax(read_csv_chunks_times),getmn(read_csv_chunks_times))
print(readcsvinchunks)

print("Reading multiple csvs")
readmultiplecsvs = """
Average read time : %f
Maximum read time : %f
Minimum read time : %f
""" % (calcavg(read_multiple_csv_times),getmax(read_multiple_csv_times),getmn(read_multiple_csv_times))
print(readmultiplecsvs)

print("DF to SQL")
dftosql = """
Average convert time : %f
Maximum convert time : %f
Minimum convert time : %f
""" % (calcavg(df_to_sql_times),getmax(df_to_sql_times),getmn(df_to_sql_times))
print(dftosql)

print("SQL Query")
querysql = """
Average query time : %f
Maximum query time : %f
Minimum query time : %f
""" % (calcavg(query_sql_times),getmax(query_sql_times),getmn(query_sql_times))
print(querysql)

print("SQL to DF")
sqltodf = """
Average df convert time : %f
Maximum df convert time : %f
Minimum df convert time : %f
""" % (calcavg(sql_to_df_times),getmax(sql_to_df_times),getmn(sql_to_df_times))
print(sqltodf)

print("DF to Pickle")
dftopkl = """
Average convert time : %f
Maximum convert time : %f
Minimum convert time : %f
""" % (calcavg(df_to_pkl_times),getmax(df_to_pkl_times),getmn(df_to_pkl_times))
print(dftopkl)

print("Pickle to DF")
pkltodf = """
Average df convert time : %f
Maximum df convert time : %f
Minimum df convert time : %f
""" % (calcavg(pkl_to_df_times),getmax(pkl_to_df_times),getmn(pkl_to_df_times))
print(pkltodf)

print("DF to Parquet Approach 1")
dftoparquetapp1 = """
Average convert time : %f
Maximum convert time : %f
Minimum convert time : %f
""" % (calcavg(df_to_parquet_app1_times),getmax(df_to_parquet_app1_times),getmn(df_to_parquet_app1_times))
print(dftoparquetapp1)

print("Parquet to DF Approach 1")
parquettodfapp1 = """
Average df convert time : %f
Maximum df convert time : %f
Minimum df convert time : %f
""" % (calcavg(parquet_to_df_app1_times),getmax(parquet_to_df_app1_times),getmn(parquet_to_df_app1_times))
print(parquettodfapp1)

print("DF to Parquet Approach 2")
dftoparquetapp2 = """
Average convert time : %f
Maximum convert time : %f
Minimum convert time : %f
""" % (calcavg(df_to_parquet_app2_times),getmax(df_to_parquet_app2_times),getmn(df_to_parquet_app2_times))
print(dftoparquetapp2)

print("Parquet to DF Approach 2")
parquettodfapp2 = """
Average df convert time : %f
Maximum df convert time : %f
Minimum df convert time : %f
""" % (calcavg(parquet_to_df_app2_times),getmax(parquet_to_df_app2_times),getmn(parquet_to_df_app2_times))
print(parquettodfapp2)

print("DF to cPickle")
dftocpkl = """
Average convert time : %f
Maximum convert time : %f
Minimum convert time : %f
""" % (calcavg(df_to_cpkl_times),getmax(df_to_cpkl_times),getmn(df_to_cpkl_times))
print(dftocpkl)

print("cPickle to DF")
cpkltodf = """
Average df convert time : %f
Maximum df convert time : %f
Minimum df convert time : %f
""" % (calcavg(cpkl_to_df_times),getmax(cpkl_to_df_times),getmn(cpkl_to_df_times))
print(cpkltodf)

print("DF to Feather")
dftofeather = """
Average convert time : %f
Maximum convert time : %f
Minimum convert time : %f
""" % (calcavg(df_to_feather_times),getmax(df_to_feather_times),getmn(df_to_feather_times))
print(dftofeather)

print("Feather to DF")
feathertodf = """
Average df convert time : %f
Maximum df convert time : %f
Minimum df convert time : %f
""" % (calcavg(feather_to_df_times),getmax(feather_to_df_times),getmn(feather_to_df_times))
print(feathertodf)

print("Query DF")
querydf = """
Average query time : %f
Maximum query time : %f
Minimum query time : %f
""" % (calcavg(query_df_times),getmax(query_df_times),getmn(query_df_times))
print(querydf)















