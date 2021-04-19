#### Functions for retrieving and formatting LitCovid Topics
import os
import requests
import pandas as pd
from pandas import read_csv
import time
from datetime import datetime
import json

        
def get_pmids(res):
    data=[]
    litcovid_data = res.text.split('\n')[34:]
    for line in litcovid_data:
        if line.startswith('#') or line.startswith('p'):
            continue
        if len(line.strip())<5:
            continue
        data.append('pmid'+line.split('\t')[0])
    return(data)


def get_topics():
    topics = {'Mechanism':'Mechanism',
              'Transmission':'Transmission',
              'Diagnosis':'Diagnosis',
              'Treatment':'Treatment',
              'Prevention':'Prevention',
              'Case%20Report':'Case Descriptions',
              'Epidemic%20Forecasting':'Forecasting'}
    pmid_dict = {}
    for topic in topics.keys():
        res = requests.get('https://www.ncbi.nlm.nih.gov/research/coronavirus-api/export/tsv?filters=%7B%22topics%22%3A%5B%22'+topic+'%22%5D%7D')
        data = get_pmids(res)
        pmid_dict[topics[topic]]=data
        time.sleep(1)
    return(pmid_dict)


def transform_dict(pmid_dict):
    new_dict = {}
    for eachkey in pmid_dict.keys():
        pmidlist = pmid_dict[eachkey]
        tmpdf = pd.DataFrame(pmidlist)
        tmpdf['topicCategory']=eachkey
        tmpdf.reset_index().rename(columns={'0':'_id'},)
        new_dict[eachkey]=tmpdf
    return(new_dict)


def merge_results(pmid_dict):
    allresults = pd.concat((pmid_dict.values()), ignore_index=True)
    allresults.rename(columns={0:"_id"},inplace=True)
    return(allresults)
 

def get_litcovid_topics(DATAPATH):
    pmid_dict = get_topics()
    clean_dict = transform_dict(pmid_dict)
    allresults = merge_results(clean_dict)
    exportpath = os.path.join(DATAPATH,'litcovidtopics.tsv')
    allresults.to_csv(exportpath,sep='\t',header=True)


