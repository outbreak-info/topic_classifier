#### Functions for building the non-litcovid topics training set from keywords
import os
import pandas as pd
from pandas import read_csv
import time
from datetime import datetime
import json
import pickle
import requests

#### Pull ids from a json file
from src.common import *



#### Keyword load functions
def get_keypath(DATAPATH):
    KEYPATH = os.path.join(DATAPATH,'keywords/')
    keydirfiles = os.listdir(KEYPATH)
    KEYFILES = [x for x in keydirfiles if '.txt' in x]
    return(KEYPATH,KEYFILES)

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


def get_subpath(DATAPATH,topic):
    keystring = 'subtopics/keywords/'+topic+'/'
    SUBPATH = os.path.join(DATAPATH,keystring)
    SUBFILES = os.listdir(SUBPATH)
    return(SUBPATH,SUBFILES)


def load_sub_terms(DATAPATH,topic):
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



#### LitCovid querying functions
## Search litcovid for a term and retrieve the pmids
def search_litcovid_ids(searchterm,topic=False):
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
def category_id_check(DATAPATH,topic=False,source='outbreak',topic_type='broadtopic'):
    if 'broad' in topic_type:
        keyword_dict = load_search_terms(DATAPATH)
    else:
        mapped_topics = {'Treatment':['Prevention','Treatment','Case Report'],
                         'Transmission':['Transmission','Prevention']}
        special_cases = {'Mechanism of Transmission':['Mechanism','Transmission']}
        keyword_dict = load_sub_terms(DATAPATH,topic)
    allids = []
    if source == 'outbreak':
        for category in keyword_dict.keys():  
            keywordlist = keyword_dict[category]
            for eachkey in keywordlist:
                idlist = get_query_ids(eachkey)
                allids.append({'category':category,'searchterm':eachkey,'ids':idlist})
        idcheck = pd.DataFrame(allids)       
    if ((source == 'litcovid') and ('broad' in topic_type)):
        for category in keyword_dict.keys():  
            keywordlist = keyword_dict[category]
            for eachkey in keywordlist:
                idlist = search_litcovid_ids(eachkey)
                time.sleep(0.5)
                allids.append({'category':category,'searchterm':eachkey,'ids':idlist})
        idcheck = pd.DataFrame(allids)
    if ((source == 'litcovid') and ('broad' not in topic_type)):
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
                        time.sleep(0.5)
                        totalids = list(set(idlist).union(set(meetsreqs)))
                        allids.append({'category':category,'searchterm':eachkey,'ids':totalids})                    
            elif topic == 'Epidemiology':
                topic = False
                for eachkey in keywordlist:
                    idlist = search_litcovid_ids(eachkey,topic)
                    time.sleep(0.5)
                    allids.append({'category':category,'searchterm':eachkey,'ids':idlist})                    
            elif topic in (mapped_topics.keys()):
                topic_sublist = mapped_topics[topic]
                for eachkey in keywordlist:
                    totalids = []
                    for eachtopic in topic_sublist:
                        idlist = search_litcovid_ids(eachkey,eachtopic)
                        time.sleep(0.5)
                        totalids = list(set(idlist).union(set(totalids)))
                    allids.append({'category':category,'searchterm':eachkey,'ids':totalids})
            else:
                for eachkey in keywordlist:
                    idlist = search_litcovid_ids(eachkey,topic)
                    time.sleep(0.5)
                    allids.append({'category':category,'searchterm':eachkey,'ids':idlist})
        idcheck = pd.DataFrame(allids)
    return(idcheck)



## Pull the id lists after search outbreak and litcovid and compare them
## keep only ids in common for training purposes
## Note, this will remove all preprints from the training set since litcovid does not have them
def get_in_common_ids(DATAPATH,topic_type='broadtopic'):
    if 'broad' in topic_type:
        outbreakids = category_id_check(DATAPATH)
        litcovidids = category_id_check(DATAPATH,source='litcovid')
        mergedf = outbreakids.merge(litcovidids,on=(['category','searchterm']),how='outer')
        mergedf['clean_ids'] = mergedf.apply(lambda row: list(set(row['ids_x']).intersection(set(row['ids_y']))),axis=1)
        mergedf['len_clean_ids'] = mergedf['clean_ids'].str.len()
        mergedf['len_ids_x'] = mergedf['ids_x'].str.len()
        mergedf['len_ids_y'] = mergedf['ids_y'].str.len()
        cleandf = mergedf.drop(columns=['ids_x','ids_y'])
    else:
        maintopics = ['Diagnosis',
                  'Epidemiology',
                  'Mechanism',
                  'Prevention',
                  'Transmission',
                  'Treatment']
        cleandf = pd.DataFrame(columns=['category','searchterm','len_ids_x','len_ids_y','len_clean_ids','clean_ids'])
        for topic in maintopics:
            outbreakids = category_id_check(DATAPATH,topic,topic_type='subtopic')
            litcovidids = category_id_check(DATAPATH,topic,source='litcovid',topic_type='subtopic')
            mergedf = outbreakids.merge(litcovidids,on=(['category','searchterm']),how='outer')
            mergedf['clean_ids'] = mergedf.apply(lambda row: list(set(row['ids_x']).intersection(set(row['ids_y']))),axis=1)
            mergedf['len_clean_ids'] = mergedf['clean_ids'].str.len()
            mergedf['len_ids_x'] = mergedf['ids_x'].str.len()
            mergedf['len_ids_y'] = mergedf['ids_y'].str.len()
            mergedf.drop(columns=['ids_x','ids_y'],inplace=True)
            cleandf = pd.concat((cleandf,mergedf),ignore_index=True)
    return(cleandf)



def generate_training_data(DATAPATH,RESULTSPATH,topic_type='broadtopic',savefile = False):
    if 'broad' in topic_type:
        cleandf = get_in_common_ids(DATAPATH,'broadtopic')
    else:
        cleandf = get_in_common_ids(DATAPATH,'subtopic')
    boom = cleandf.explode('clean_ids')
    boomclean = boom[['category','clean_ids']].copy()
    boomclean.drop_duplicates(keep='first',inplace=True)
    trainingdf = boomclean.groupby('category')['clean_ids'].apply(list)
    boomclean.rename(columns={'category':'topicCategory','clean_ids':'_id'},inplace=True)
    if savefile == False:
        return(boomclean)
    else:
        with open(os.path.join(RESULTSPATH,"trainingdf.pickle"), "wb") as outfile: 
            pickle.dump(trainingdf, outfile) 

            
            
def get_other_topics(DATAPATH,RESULTSPATH):
    trainingdf = generate_training_data(DATAPATH,RESULTSPATH,'broadtopic')
    trainingdf.to_csv(os.path.join(DATAPATH,'othertopics.tsv'),sep='\t',header=True)
    

