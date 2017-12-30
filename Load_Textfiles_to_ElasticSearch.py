#Importing the necessary packages
import os
from pyelasticsearch import ElasticSearch
import pandas as pd
from time import time
from multiprocessing.pool import Pool

raw_data_path="D:/Countries"

# Function to insert record in ES in bulk
def ES_bulk_insert(file_name):
    index_name="geodata"
    doc_type="data"
	
    # ElasticSearch URL
    ElasticSearch_URL="http://localhost:9200/"
          
    file_path_name=raw_data_path+"/"+file_name
    
    t0=time()
    
    #Bulk size to be import the records in elasticsearch
    chunk_size=5000
    
    txt_file=pd.read_csv(file_path_name, sep="\t",iterator=True,chunksize=chunk_size, header=None, names = ['geonameid', 'name', 'asciiname', 'alternatenames','latitude', 'longitude', 'feature_class', 'feature_code', 'country_code','cc2','admin1_code', 'admin2_code', 'admin3_code', 'admin4_code', 'population', 'elevation', 'dem', 'timezone', 'modification_date'],dtype ={"geonameid" : int64, "name" : object , "asciiname" : object , "alternatenames" : object,"latitude" : float64, "longitude" : float64, "feature_class" : object, "feature_code" : object, "country_code" :object ,"cc2" : object,"admin1_code" : object, "admin2_code" : object, "admin3_code" : object, "admin4_code" : object, "population" : int64, "elevation" : object, "dem" : int64, "timezone" : object, "modification_date" : object}) 

    # Connecting to ElasticSearch
    es = ElasticSearch(ElasticSearch_URL)
    print("Data Import started for file ",file_name)
    
    #Insert the Dataframe to elasticsearch using bulk
    for i,df in enumerate(txt_file): 
        print(i)
        records=df.where(pd.notnull(df), None).T.to_dict()
        list_records=[records[it] for it in records]
        try :
            es.bulk_index(index_name,doc_type,list_records)
        except:
            print("Error.. Skipping some records")
            pass

    print ("File ",file_name,"imported in %.3fs"%(time()-t0))


if __name__ == '__main__':
    # Get list of files
    files_list=os.listdir(raw_data_path)
    print(files_list)
   
    #Parameter number of parallel process
    num_of_processes=3
    with Pool(num_of_processes) as pool:
        pool.map(ES_bulk_insert, files_list)
        pool.close()
        pool.join()