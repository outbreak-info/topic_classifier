import os
import pandas as pd
from pandas import read_csv
import time
from datetime import datetime
import json
import pickle


from src.common import *
from src.fetch_offtopics import *





def get_sub_topics(DATAPATH,RESULTSPATH,inspectfile=False):
    trainingdf = generate_training_data(DATAPATH,RESULTSPATH,'subtopic')
    trainingdf.to_csv(os.path.join(DATAPATH,'subtopics.tsv'),sep='\t',header=True)
    if inspectfile==True:
        return(trainingdf)

    
def search_keywords(keywordlist,keywordsdf):
    idset=set()
    for eachkeyword in keywordlist:
        keyids = keywordsdf['_id'].loc[keywordsdf['keywords'].str.contains(eachkeyword,regex=False)].unique().tolist()
        idset = idset.union(set(keyids))
    idlist = list(idset)
    return(idlist)


#### Note that applying the str.contains with regex=False requires looping (accomplished via the search_keywords function)
#### But is much faster than converting the search terms to a regex and then doing the str.contains lookup
def map_keywords(DATAPATH):
    SUBDATAPATH = os.path.join(DATAPATH,'subtopics/')
    litcovid_ids = get_pub_ids("litcovid")
    textdf = batch_fetch_keywords(litcovid_ids)
    keywordsdf = textdf.loc[textdf['keywords'].str.len()>1].copy()
    curated_pmids = read_csv(os.path.join(SUBDATAPATH,'pmids_for_training.tsv'),sep='\t',header=0,index_col=0)
    curated_pmids['search terms'] = curated_pmids['search terms'].str.replace(',  ',',').str.replace(', ',',')
    curated_pmids['search terms'] = curated_pmids['search terms'].str.replace("'","")
    curated_pmids['search terms'] = curated_pmids['search terms'].str.replace('[','').str.replace(']','')
    curated_pmids['search_list'] = curated_pmids['search terms'].str.split(',')
    curated_pmids.drop(columns=['matching_pmids','no of samples','search terms'],inplace=True)
    curated_pmids['matching_pmids'] = [search_keywords(x,keywordsdf) for x in curated_pmids['search_list']]
    curated_pmids.rename(columns={'search_list':'search terms'},inplace=True)
    curated_pmids['no of samples'] = curated_pmids['matching_pmids'].str.len()   
    with open(os.path.join(SUBDATAPATH,'subtopic_pmids_for_training.pickle'),"wb") as dumpfile:
        pickle.dump(curated_pmids,dumpfile)
