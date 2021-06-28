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
from src.common import *
from src.fetch_subtopics import load_clin_cats_data

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
            predictiondf.drop_duplicates(keep='first',inplace=True)
            if newonly == True:
                predictiondf.to_csv(os.path.join(PREDICTPATH,eachtopic+"_"+eachclassifier+'.tsv'),mode='a',sep='\t',header=True)
            else:
                predictiondf.to_csv(os.path.join(PREDICTPATH,eachtopic+"_"+eachclassifier+'.tsv'),sep='\t',header=True) 
            

def get_agreement(PREDICTPATH,eachtopic,classifierlist):
    classresult = pd.DataFrame(columns=['_id','prediction','topicCategory','classifier'])
    for eachclass in classifierlist:
        tmpfile = read_csv(os.path.join(PREDICTPATH,eachtopic+"_"+eachclass+".tsv"),delimiter='\t',header=0,index_col=0)
        classresult = pd.concat((classresult,tmpfile),ignore_index=True)
    classresult.drop_duplicates(keep='first',inplace=True)
    posresults = classresult.loc[classresult['prediction']=='in category']
    agreement = posresults.groupby(['_id','topicCategory'])['classifier'].apply(list).reset_index(name='pos_pred_algorithms')
    agreement['pos_pred_count'] = agreement['pos_pred_algorithms'].str.len()
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


def merge_predictions(PREDICTPATH,topic_dict,classifierlist,agreetype='perfect'):
    topiclist = list(set(topic_dict['broadtopics']).union(set(topic_dict['subtopics'])))
    agreement = filter_agreement(PREDICTPATH,topiclist,classifierlist,agreetype='perfect')
    agreement.drop_duplicates(inplace=True,keep="first")
    return(agreement)
    
    
def classify_pubs(MODELPATH,PUBPREDICTPATH,all_ids,topic_dict,classifiers,newonly = True):
    SUBMODELPATH = os.path.join(MODELPATH,'subtopics/')
    topiclist = topic_dict['broadtopics']
    subtopiclist = topic_dict['subtopics']
    alldf = batch_fetch_meta(all_ids)
    alldata = merge_texts(alldf)    
    classifierlist = classifiers.keys()
    if newonly == True:
        predict_class(MODELPATH,PUBPREDICTPATH,topiclist,classifierlist,alldata,True)
        predict_class(SUBMODELPATH,PUBPREDICTPATH,subtopiclist,classifierlist,alldata,True)
    else:
        predict_class(MODELPATH,PUBPREDICTPATH,topiclist,classifierlist,alldata,False)
        predict_class(SUBMODELPATH,PUBPREDICTPATH,subtopiclist,classifierlist,alldata,False)

        
def check_for_new(RESULTSPATH,topicsdf,sourceset):
    oldresults = read_csv(os.path.join(RESULTSPATH,'topicCats.tsv'),delimiter='\t',header=0,index_col=0)
    all_ids = oldresults['_id'].unique().tolist()
    updated_pubslist = get_pub_ids(sourceset)
    new_pubs_only = [x for x in updated_pubslist if x not in all_ids]
    topics_ids = topicsdf['_id'].unique().tolist()
    new_topic_ids = [x for x in topics_ids if x not in all_ids]
    return(new_pubs_only,new_topic_ids)


def classify_clins(DATAPATH,MODELPATH,PREDICTPATH,classifiers,topic_dict):
    with open(os.path.join(DATAPATH,'clin_meta.pickle'),'rb') as tmpfile:
        clin_meta = pickle.load(tmpfile)
    ct_subtopics = load_clin_cats_data(os.path.join(DATAPATH,'subtopics/'))
    topiclist = topic_dict['broadtopics']
    subtopiclist = topic_dict['subtopics']
    uncategorized_cts = clin_meta.loc[~clin_meta['_id'].isin(ct_subtopics['_id'].unique().tolist())]
    has_sufficient_info = uncategorized_cts.loc[uncategorized_cts['text'].astype(str).str.len()>20]
    classifierlist = classifiers.keys()
    SUBMODELPATH = os.path.join(MODELPATH,'subtopics/')
    CLINPREDICTPATH = os.path.join(PREDICTPATH,'clinpredict/')
    predict_class(SUBMODELPATH,CLINPREDICTPATH,subtopiclist,classifierlist,has_sufficient_info,False)
    predict_class(MODELPATH,CLINPREDICTPATH,topiclist,classifierlist,has_sufficient_info,False)


#### Deal with Risk Factors and Case Descriptions which had to be dealt with differently due to litcovid
def include_clin(df):
    df.dropna()
    clins = df.loc[(df['topicCategory']=='Risk Factors')|(df['topicCategory']=='Case Descriptions')]
    explicit_clins = df['_id'].loc[df['topicCategory']=='Clinical']
    missing_clins = clins['_id'].loc[~clins['_id'].isin(explicit_clins)].to_frame(name='_id')
    missing_clins['topicCategory'] = 'Clinical'
    df = pd.concat((df,missing_clins),ignore_index=True)
    df.sort_values('_id',ascending=True,inplace=True)
    df.drop_duplicates(keep='first',inplace=True)
    return(df)
    
    
    
def load_annotations(DATAPATH,MODELPATH,PREDICTPATH,RESULTSPATH,topicsdf,classifiers,newonly = True):
    from src.common import topic_dict
    classifierlist = classifiers.keys()
    CLINPREDICTPATH = os.path.join(PREDICTPATH,'clinpredict/')
    PUBPREDICTPATH = os.path.join(PREDICTPATH,'pubpredict/')
    classify_clins(DATAPATH,MODELPATH,PREDICTPATH,classifiers,topic_dict)
    clin_total_agree = merge_predictions(CLINPREDICTPATH,topic_dict,classifierlist,agreetype='perfect')
    if newonly==True:
        new_pubs_only,new_topic_ids = check_for_new(RESULTSPATH,topicsdf,"nonlitcovid")
        all_new_ids = list(set(new_pubs_only).union(set(new_topic_ids)))
        classify_pubs(MODELPATH,PUBPREDICTPATH,new_pubs_only,topic_dict,classifiers)
        total_agree = merge_predictions(PUBPREDICTPATH,topic_dict,classifierlist,'perfect')
        new_total_agree = total_agree.loc[total_agree['_id'].isin(new_pubs_only)].copy()
        new_topics_df = topicsdf.loc[topicsdf['_id'].isin(new_topic_ids)].copy()
        totalnewresults = pd.concat((new_total_agree,new_topics_df,clin_total_agree),ignore_index=True)
        allnewresults = include_clin(totalnewresults)
        allnewresults['topicCategory'] = allnewresults['topicCategory'].str.replace('-','/')
        allnewresults.dropna(axis=0,inplace=True)
        allnewresults.reset_index(drop=True)
        cleanresults = clean_results(allnewresults)
        cleanresults.to_csv(os.path.join(RESULTSPATH,'topicCats.tsv'),mode='a',sep='\t',header=True)
    else:
        all_ids = get_pub_ids(sourceset="nonlitcovid")
        classify_pubs(MODELPATH,PUBPREDICTPATH,all_ids,topic_dict,classifiers,False)
        total_agree = merge_predictions(PUBPREDICTPATH,topic_dict,classifierlist,'perfect')
        totalresults = pd.concat((total_agree,topicsdf,clin_total_agree),ignore_index=True)
        allresults = include_clin(totalresults)
        allresults['topicCategory'] = allresults['topicCategory'].str.replace('-','/')
        allresults.dropna(axis=0,inplace=True)
        allresults.reset_index(drop=True)
        cleanresults = clean_results(allresults) 
        cleanresults.to_csv(os.path.join(RESULTSPATH,'topicCats.tsv'),mode='w',sep='\t',header=True)
    updated_results = read_csv(os.path.join(RESULTSPATH,'topicCats.tsv'),delimiter='\t',header=0,index_col=0)
    updated_results.drop_duplicates(subset='_id',keep='first',inplace=True)
    updated_results.to_csv(os.path.join(RESULTSPATH,'topicCats.tsv'),sep='\t',header=True)
    updated_results.to_json(os.path.join(RESULTSPATH,'topicCats.json'), orient='records')

    
    
