#### Functions for building the non-litcovid topics training set from keywords
import os
import requests
import pandas as pd
from pandas import read_csv
import time
from datetime import datetime
import json
import pickle

#### Pull ids from a json file
from src.common import get_ids_from_json


def get_keypath(DATAPATH):
    KEYPATH = os.path.join(DATAPATH,'keywords/')
    KEYFILES = os.listdir(KEYPATH)
    return(KEYPATH,KEYFILES)


def fetch_query_size(query):
    pubmeta = requests.get('https://api.outbreak.info/resources/query?q=(("'+query+'") AND (@type:Publication))&size=0&aggs=@type')
    pubjson = json.loads(pubmeta.text)
    pubcount = int(pubjson["facets"]["@type"]["total"])
    return(pubcount)

#### Ping the API and get all the ids for a specific source and scroll through the source until number of ids matches meta
def get_query_ids(query):
    query_size = fetch_query_size(query)
    r = requests.get('https://api.outbreak.info/resources/query?q=(("'+query+'") AND (@type:Publication))&fields=_id&fetch_all=true')
    response = json.loads(r.text)
    idlist = get_ids_from_json(response)
    try:
        scroll_id = response["_scroll_id"]
        while len(idlist) < query_size:
            r2 = requests.get('https://api.outbreak.info/resources/query?q=(("'+query+'") AND (@type:Publication))&fields=_id&fetch_all=true&scroll_id='+scroll_id)
            response2 = json.loads(r2.text)
            idlist2 = set(get_ids_from_json(response2))
            tmpset = set(idlist)
            idlist = tmpset.union(idlist2)
            try:
                scroll_id = response2["_scroll_id"]
            except:
                print("no new scroll id")
        return(idlist)
    except:
        return(idlist)


def load_search_terms(DATAPATH):
    KEYPATH,KEYFILES = get_keypath(DATAPATH)
    keyword_dict = {}
    for eachfile in KEYFILES:
        filename = eachfile.split('.')[0]
        keywords = []
        with open(os.path.join(KEYPATH,eachfile),'r') as readfile:
            for eachline in readfile:
                keywords.append(eachline.strip())
        keyword_dict[filename]=keywords
    return(keyword_dict)


def load_category_ids(DATAPATH):
    keyword_dict = load_search_terms(DATAPATH)
    cat_dict = {}
    for category in keyword_dict.keys():
        allids = []
        idlist = []
        keywordlist = keyword_dict[category]
        for eachkey in keywordlist:
            idlist = get_query_ids(eachkey)
            allids = list(set(allids).union(set(idlist)))
        cat_dict[category]=allids
    return(cat_dict)



## Search litcovid for a term and retrieve the pmids
def search_litcovid_ids(searchterm):
    check_litcovid = requests.get('https://www.ncbi.nlm.nih.gov/research/coronavirus-api/export/tsv?text="'+searchterm+'"&filters={}')
    litcovid_data = check_litcovid.text.split('\n')[34:]
    pmids = []
    for line in litcovid_data:
        if line.startswith('#') or line.startswith('p'):
            continue
        pmids.append(line.split('\t')[0])
    cleanpmids = ['pmid'+x for x in pmids if x != ""]
    return(cleanpmids)

## load the list of search terms for each topicCategory and push it to either outbreak (default) or litcovid
## returns an id list
def category_id_check(DATAPATH,source='outbreak'):
    keyword_dict = load_search_terms(DATAPATH)
    allids = []
    if source == 'litcovid':
        for category in keyword_dict.keys():  
            keywordlist = keyword_dict[category]
            for eachkey in keywordlist:
                idlist = search_litcovid_ids(eachkey)
                allids.append({'category':category,'searchterm':eachkey,'ids':idlist})
        idcheck = pd.DataFrame(allids)
    else:
        for category in keyword_dict.keys():  
            keywordlist = keyword_dict[category]
            for eachkey in keywordlist:
                idlist = get_query_ids(eachkey)
                allids.append({'category':category,'searchterm':eachkey,'ids':idlist})
        idcheck = pd.DataFrame(allids)
    return(idcheck)


## Pull the id lists after search outbreak and litcovid and compare them
## keep only ids in common for training purposes
## Note, this will remove all preprints from the training set since litcovid does not have them
def get_in_common_ids(DATAPATH):
    outbreakids = category_id_check(DATAPATH)
    litcovidids = category_id_check(DATAPATH,source='litcovid')
    mergedf = outbreakids.merge(litcovidids,on=(['category','searchterm']),how='outer')
    i=0
    tmplist = []
    while i <len(mergedf):
        idsincommon = list(set(mergedf.iloc[i]['ids_x']).intersection(set(mergedf.iloc[i]['ids_y'])))
        tmplist.append({'category':mergedf.iloc[i]['category'],
                        'searchterm':mergedf.iloc[i]['searchterm'],
                        'len_ids_x':len(mergedf.iloc[i]['ids_x']),
                        'len_ids_y':len(mergedf.iloc[i]['ids_y']),
                        'len_clean_ids':len(idsincommon),
                        'clean_ids':idsincommon})
        i=i+1
    cleandf = pd.DataFrame(tmplist)
    return(cleandf)   


def generate_training_dict(DATAPATH,RESULTSPATH,savefile = False):
    cleandf = get_in_common_ids(DATAPATH)
    training_dict = {}
    for eachcat in cleandf['category'].unique().tolist(): 
        j=0
        tmpdf = cleandf.loc[cleandf['category']==eachcat]
        allids = []
        while j<len(tmpdf):
            allids = list(set(allids).union(set(tmpdf.iloc[j]['clean_ids'])))
            j=j+1
        training_dict[eachcat]=allids
    if savefile == False:
        return(training_dict)
    else:
        with open(os.path.join(RESULTSPATH,"training_dict.json"), "w") as outfile: 
            json.dump(training_dict, outfile)        

def transform_training_dict(training_dict):
    trainingdf = pd.DataFrame(columns=['_id','topicCategory'])
    for eachcat in training_dict.keys():
        idlist = pd.DataFrame(training_dict[eachcat])
        idlist.rename(columns={0:'_id'},inplace=True)
        idlist['topicCategory']=eachcat
        trainingdf = pd.concat((trainingdf,idlist),ignore_index=True)
    return(trainingdf)


def get_other_topics(DATAPATH,RESULTSPATH):
    training_dict = generate_training_dict(DATAPATH,RESULTSPATH)
    trainingdf = transform_training_dict(training_dict)
    trainingdf.to_csv(os.path.join(DATAPATH,'othertopics.tsv'),sep='\t',header=True)
    

