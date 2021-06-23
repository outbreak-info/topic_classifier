import os
import requests
import pandas as pd
from pandas import read_csv
import time
from datetime import datetime
import json
import pickle

#### Pull ids from a json file
from src.common import *
from src.fetch_offtopics import *


def get_subpath(DATAPATH,topic):
    keystring = 'subtopics/keywords/'+topic+'/'
    SUBPATH = os.path.join(DATAPATH,keystring)
    SUBFILES = os.listdir(SUBPATH)
    return(SUBPATH,SUBFILES)


def load_search_terms(DATAPATH,topic):
    SUBPATH,SUBFILES = get_subpath(DATAPATH,topic)
    keyword_dict = {}
    for eachfile in SUBFILES:
        filename = eachfile.split('.')[0]
        keywords = []
        with open(os.path.join(SUBPATH,eachfile),'r') as readfile:
            for eachline in readfile:
                keywords.append(eachline.strip())
        keyword_dict[filename]=keywords
    return(keyword_dict)



## Search litcovid for a term and retrieve the pmids
def search_litcovid_ids(searchterm,topic):
    baseurl = 'https://www.ncbi.nlm.nih.gov/research/coronavirus-api/export/tsv?text="'
    filterurl = '"&filters={"topics":["'
    urlend = '"]}'
    nofilter = '"&filters={}'
    if topic==False:
        litsearchurl = baseurl+searchterm+nofilter
    else:
        litsearchurl = baseurl+searchterm+filterurl+topic+urlend
    check_litcovid = requests.get(litsearchurl)
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
def sub_category_id_check(DATAPATH,topic,source='outbreak'):
    mapped_topics = {'Treatment':['Prevention','Treatment','Case Report'],
                     'Transmission':['Transmission','Prevention'],
                     'Prevention':['Prevention','Case Report']}
    special_cases = {'Mechanism of Transmission':['Mechanism','Transmission']}
    keyword_dict = load_search_terms(DATAPATH,topic)
    allids = []
    if source == 'litcovid':
        for category in keyword_dict.keys():
            keywordlist = keyword_dict[category]
            if category=='Mechanism of Transmission':
                litcovidtopics = read_csv(os.path.join(DATAPATH,'litcovidtopics.tsv'),delimiter='\t',index_col=0,header=0)
                mechtrans = litcovidtopics.loc[(litcovidtopics['topicCategory']=='Mechanism')|
                                               (litcovidtopics['topicCategory']=='Transmission')].copy()
                mechtrans.drop_duplicates(keep='first',inplace=True)
                freqs = mechtrans.groupby('_id').size().reset_index(name='counts')
                meetsreqs = freqs['_id'].loc[freqs['counts']>1].unique().tolist()
                for category in keyword_dict.keys():  
                    keywordlist = keyword_dict[category]
                    for eachkey in keywordlist:
                        idlist = search_litcovid_ids(eachkey,topic)
                        totalids = list(set(idlist).union(set(meetsreqs)))
                        allids.append({'category':category,'searchterm':eachkey,'ids':totalids})                    
            else:
                if topic == 'Epidemiology':
                    topic = False
                    for eachkey in keywordlist:
                        idlist = search_litcovid_ids(eachkey,topic)
                        allids.append({'category':category,'searchterm':eachkey,'ids':idlist})                    
                elif topic in (mapped_topics.keys()):
                    topic_sublist = mapped_topics[topic]
                    for eachkey in keywordlist:
                        totalids = []
                        for eachtopic in topic_sublist:
                            idlist = search_litcovid_ids(eachkey,eachtopic)
                            totalids = list(set(idlist).union(set(totalids)))
                        allids.append({'category':category,'searchterm':eachkey,'ids':totalids})
                else:
                    for eachkey in keywordlist:
                        idlist = search_litcovid_ids(eachkey,topic)
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
def get_in_common_sub_ids(DATAPATH):
    maintopics = ['Diagnosis',
              'Epidemiology',
              'Mechanism',
              'Prevention',
              'Transmission',
              'Treatment']
    tmplist = []
    for topic in maintopics:
        outbreakids = sub_category_id_check(DATAPATH,topic)
        litcovidids = sub_category_id_check(DATAPATH,topic,source='litcovid')
        mergedf = outbreakids.merge(litcovidids,on=(['category','searchterm']),how='outer')
        i=0
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


def generate_subtraining_dict(DATAPATH,RESULTSPATH,savefile = False):
    cleandf = get_in_common_sub_ids(DATAPATH)
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


def get_sub_topics(DATAPATH,RESULTSPATH):
    training_dict = generate_subtraining_dict(DATAPATH,RESULTSPATH)
    trainingdf = transform_training_dict(training_dict)
    trainingdf.to_csv(os.path.join(DATAPATH,'subtopics.tsv'),sep='\t',header=True)
    

def map_keywords(DATAPATH):
    SUBDATAPATH = os.path.join(DATAPATH,'subtopics/')
    litcovid_ids = get_pub_ids("litcovid")
    textdf = batch_fetch_keywords(litcovid_ids)
    keywordsdf = textdf.loc[textdf['keywords'].str.len()>1].copy()
    curated_pmids = read_csv(os.path.join(SUBDATAPATH,'pmids_for_training.tsv'),sep='\t',header=0,index_col=0)
    i=0
    new_curated_pmids = []
    while i < len(curated_pmids):
        matching_pmids = []
        topicCat = curated_pmids.iloc[i]['topicCategory']
        category = curated_pmids.iloc[i]['category']
        searchterm_split = curated_pmids.iloc[i]['search terms'].split(',')
        search_terms = [x.strip() for x in searchterm_split]
        for eachterm in search_terms:
            try:
                tmpdf = keywordsdf.loc[keywordsdf['keywords'].astype(str).str.lower().str.contains(eachterm)]
                pmids = tmpdf['_id'].unique().tolist()
                matching_pmids = list(set(pmids).union(set(matching_pmids)))
            except:
                continue
        new_curated_pmids.append({'topicCategory':topicCat,'category':category,
                                  'description':curated_pmids.iloc[i]['description'],
                                  'subcategory':curated_pmids.iloc[i]['subcategory'],
                                  'search terms':search_terms,'matching_pmids':matching_pmids,
                                  'no of samples':len(matching_pmids)})
        i=i+1
    new_curated_pmids_df = pd.DataFrame(new_curated_pmids)     
    with open(os.path.join(SUBDATAPATH,'subtopic_pmids_for_training.pickle'),"wb") as dumpfile:
        pickle.dump(new_curated_pmids_df,dumpfile)
