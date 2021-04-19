import os
import requests
import pandas as pd
from pandas import read_csv
import time
from datetime import datetime
import json
import pickle
import sklearn
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, roc_auc_score


#### This function classifies ONLY publications which are "new" and have not yet been classified
#### It uses pre-existing trained models, so if the models have been changed/updated, run the other script

#### Pull ids from a json file
from src.common import get_ids_from_json
from src.common import clean_results
from src.common import batch_fetch_meta
from src.common import merge_texts

#### Get the size of the source (to make it easy to figure out when to stop scrolling)
def fetch_src_size(source):
    pubmeta = requests.get("https://api.outbreak.info/resources/query?q=((@type:Publication) AND (curatedBy.name:"+source+"))&size=0&aggs=@type")
    pubjson = json.loads(pubmeta.text)
    pubcount = int(pubjson["facets"]["@type"]["total"])
    return(pubcount)


#### Ping the API and get all the ids for a specific source and scroll through the source until number of ids matches meta
def get_source_ids(source):
    source_size = fetch_src_size(source)
    r = requests.get("https://api.outbreak.info/resources/query?q=((@type:Publication) AND (curatedBy.name:"+source+"))&fields=_id&fetch_all=true")
    response = json.loads(r.text)
    idlist = get_ids_from_json(response)
    try:
        scroll_id = response["_scroll_id"]
        while len(idlist) < source_size:
            r2 = requests.get("https://api.outbreak.info/resources/query?q=((@type:Publication) AND (curatedBy.name:"+source+"))&fields=_id&fetch_all=true&scroll_id="+scroll_id)
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


def get_pub_ids(sourceset):
    pub_srcs = {"preprint":["bioRxiv","medRxiv"],"litcovid":["litcovid"],
                "other":["Figshare","Zenodo","MRC Centre for Global Infectious Disease Analysis"],
                "all":["Figshare","Zenodo","MRC Centre for Global Infectious Disease Analysis",
                       "bioRxiv","medRxiv","litcovid"]}
    sourcelist = pub_srcs[sourceset]
    allids = []
    for eachsource in sourcelist:
        sourceids = get_source_ids(eachsource)
        allids = list(set(allids).union(set(sourceids)))
    return(allids)

def load_vectorizer(MODELPATH,category):
    vectorizerfile = os.path.join(MODELPATH,"vectorizer_"+category+".pickle")
    vectorizer = pickle.load(open(vectorizerfile,'rb'))
    return(vectorizer)

def predict_class(MODELPATH,PREDICTPATH,topiclist,classifierlist,df,newonly = True):
    labels = df['_id']
    for eachtopic in topiclist:
        vectorizer = load_vectorizer(MODELPATH,eachtopic)
        M = vectorizer.transform(df['text'])
        for eachclassifier in classifierlist:
            classifierfile = os.path.join(MODELPATH, eachclassifier+"_"+eachtopic+'.sav')
            classifier = pickle.load(open(classifierfile, 'rb'))
            prediction = classifier.predict(M)
            list_of_tuples = list(zip(labels,prediction))
            predictiondf = pd.DataFrame(list_of_tuples, columns = ['_id', 'prediction'])
            predictiondf['topicCategory']=eachtopic
            predictiondf['classifier']=eachclassifier
            if newonly == True:
                predictiondf.to_csv(os.path.join(PREDICTPATH,eachtopic+"_"+eachclassifier+'.tsv'),mode='a',sep='\t',header=True)
            else:
                predictiondf.to_csv(os.path.join(PREDICTPATH,eachtopic+"_"+eachclassifier+'.tsv'),sep='\t',header=True) 
            

def get_agreement(PREDICTPATH,eachtopic,classifierlist):
    agreement = pd.DataFrame(columns=['_id','topicCategory','pos_pred_count','pos_pred_algorithms'])
    classresult = pd.DataFrame(columns=['_id','prediction','topicCategory','classifier'])
    for eachclass in classifierlist:
        tmpfile = read_csv(os.path.join(PREDICTPATH,eachtopic+"_"+eachclass+".tsv"),delimiter='\t',header=0,index_col=0)
        classresult = pd.concat((classresult,tmpfile),ignore_index=True)
    posresults = classresult.loc[classresult['prediction']=='in category']
    agreecounts = posresults.groupby('_id').size().reset_index(name='counts')
    no_agree = posresults.loc[posresults['_id'].isin(agreecounts['_id'].loc[agreecounts['counts']==1].tolist())].copy()
    no_agree.rename(columns={'classifier':'pos_pred_algorithms'},inplace=True)
    no_agree['pos_pred_count']=1
    no_agree.drop('prediction',axis=1,inplace=True)
    perfect_agree = posresults.loc[posresults['_id'].isin(agreecounts['_id'].loc[agreecounts['counts']==len(classifierlist)].tolist())].copy()
    perfect_agree['pos_pred_count']=len(classifierlist)
    perfect_agree['pos_pred_algorithms']=str(classifierlist)
    perfect_agree.drop(['prediction','classifier'],axis=1,inplace=True)
    perfect_agree.drop_duplicates('_id',keep='first',inplace=True)
    partialcountids = agreecounts['_id'].loc[((agreecounts['counts']>1)&
                                          (agreecounts['counts']<len(classifierlist)))].tolist()
    tmplist = []
    for eachid in list(set(partialcountids)):
        tmpdf = posresults.loc[posresults['_id']==eachid]
        tmpdict = {'_id':eachid,'topicCategory':eachtopic,'pos_pred_count':len(tmpdf),
                   'pos_pred_algorithms':str(tmpdf['classifier'].tolist())}
        tmplist.append(tmpdict)
    partial_agree = pd.DataFrame(tmplist)    
    agreement = pd.concat((agreement,no_agree,partial_agree,perfect_agree),ignore_index=True)
    return(agreement)

def filter_agreement(PREDICTPATH,topiclist,classifierlist,agreetype='perfect'):
    allagreement = pd.DataFrame(columns=['_id','topicCategory','pos_pred_count','pos_pred_algorithms'])
    for eachtopic in topiclist:
        agreement = get_agreement(PREDICTPATH,eachtopic,classifierlist)
        allagreement = pd.concat((allagreement,agreement),ignore_index=True)
    if agreetype=='perfect':
        filtered_agreement = allagreement[['_id','topicCategory']].loc[allagreement['pos_pred_count']==len(classifierlist)].copy()
    elif agreetype=='None':
        filtered_agreement = allagreement[['_id','topicCategory']].loc[allagreement['pos_pred_count']==1].copy()
    else:
        partialcountids = allagreement['_id'].loc[((allagreement['pos_pred_count']>1)&
                                          (allagreement['pos_pred_count']<len(classifierlist)))].tolist()
        filtered_agreement = allagreement[['_id','topicCategory']].loc[allagreement['_id'].isin(partialcountids)].copy()
    return(filtered_agreement)


def merge_predictions(PREDICTPATH,topiclist,classifierlist,agreetype='perfect'):
    agreement = filter_agreement(PREDICTPATH,topiclist,classifierlist,agreetype='perfect')
    agreement.drop_duplicates(inplace=True,keep="first")
    return(agreement)
    
def classify_pubs(MODELPATH,PREDICTPATH,all_ids,topiclist,classifiers,newonly = True):
    alldf = batch_fetch_meta(all_ids)
    alldata = merge_texts(alldf)    
    classifierlist = classifiers.keys()
    if newonly == True:
        predict_class(MODELPATH,PREDICTPATH,topiclist,classifierlist,alldata,newonly = True)
    else:
        predict_class(MODELPATH,PREDICTPATH,topiclist,classifierlist,alldata,newonly = False)

def check_for_new(RESULTSPATH,topicsdf):
    oldresults = read_csv(os.path.join(RESULTSPATH,'topicCats.tsv'),delimiter='\t',header=0,index_col=0)
    all_ids = oldresults['_id'].unique().tolist()
    updated_pubslist = get_pub_ids(sourceset="other")
    new_pubs_only = [x for x in updated_pubslist if x not in all_ids]
    topics_ids = topicsdf['_id'].unique().tolist()
    new_topic_ids = [x for x in topics_ids if x not in oldresults]
    return(new_pubs_only,new_topic_ids)

    
def load_annotations(MODELPATH,PREDICTPATH,RESULTSPATH,topicsdf,classifiers,newonly = True):
    topiclist = topicsdf['topicCategory'].unique().tolist()
    classifierlist = classifiers.keys()
    if newonly==True:
        new_pubs_only,new_topic_ids = check_for_new(RESULTSPATH,topicsdf)
        all_new_ids = list(set(new_pubs_only).union(set(new_topic_ids)))
        classify_pubs(MODELPATH,PREDICTPATH,new_pubs_only,topiclist,classifiers)
        total_agree = merge_predictions(PREDICTPATH,topiclist,classifierlist,agreetype='perfect')
        new_total_agree = total_agree.loc[total_agree['_id'].isin(new_pubs_only)].copy()
        new_topics_df = topicsdf.loc[topicsdf['_id'].isin(new_topic_ids)].copy()
        allnewresults = pd.concat((new_total_agree,new_topics_df),ignore_index=True)
        cleanresults = clean_results(allnewresults)
    else:
        all_ids = get_pub_ids(sourceset="other")
        classify_pubs(MODELPATH,PREDICTPATH,all_ids,topiclist,classifiers,newonly = False)
        total_agree = merge_predictions(PREDICTPATH,topiclist,classifierlist,agreetype='perfect')
        allresults = pd.concat((total_agree,topicsdf),ignore_index=True)
        cleanresults = clean_results(allresults)    
    cleanresults.to_csv(os.path.join(RESULTSPATH,'topicCats.tsv'),mode='a',sep='\t',header=True)
    updated_results = read_csv(os.path.join(RESULTSPATH,'topicCats.tsv'),delimiter='\t',header=0,index_col=0)
    updated_results.drop_duplicates(subset='_id',keep='first',inplace=True)
    updated_results.to_csv(os.path.join(RESULTSPATH,'topicCats.tsv'),mode='a',sep='\t',header=True)
    updated_results.to_json(os.path.join(RESULTSPATH,'topicCats.json'), orient='records')

    
    
