import os
import pathlib
import pickle
import pandas as pd
from pandas import read_csv

from src.common import *




def load_lit_kw_subcats(SUBDATAPATH):
    with open(os.path.join(SUBDATAPATH,'subtopic_pmids_for_training.pickle'),'rb') as litfile:
        litkeytopics = pickle.load(litfile)
    keysubtopics = litkeytopics.loc[litkeytopics['subcategory']==True]
    return(keysubtopics)


def load_lit_subcats(DATAPATH):
    SUBDATAPATH = os.path.join(DATAPATH,'subtopics/')
    litsubtopicsfile = os.path.join(DATAPATH,'subtopics.tsv')
    litsubtopics = read_csv(litsubtopicsfile,delimiter='\t',header=0,index_col=0)
    litsubtopics['topicCategory'] = litsubtopics['topicCategory'].astype(str).str.replace('-','/')
    keysubtopics = load_lit_kw_subcats(SUBDATAPATH)
    boom = keysubtopics.explode('matching_pmids').reset_index(drop=True)
    boom.rename(columns={'matching_pmids':'_id'},inplace=True)
    boom_clean = boom[['_id','topicCategory']].copy()
    boom_clean['topicCategory'] = boom_clean['topicCategory'].astype(str).str.replace(' / ','/')
    rawpmidlist = list(set(boom_clean['_id'].unique().tolist()).union(set(litsubtopics['_id'].unique().tolist())))
    pmidlist = list(filter(None,[str(x) for x in rawpmidlist]))
    textdf = batch_fetch_meta(pmidlist)
    textdf = merge_texts(textdf)
    clean_textdf = textdf[['_id','text']]
    combidf = pd.concat((boom_clean,litsubtopics[['_id','topicCategory']]),ignore_index=True)
    litmergeddf = combidf.merge(textdf,on='_id',how='left')
    return(litmergeddf)


def load_citsci_data(SUBDATAPATH):
    with open(os.path.join(SUBDATAPATH,'curated_training_df.pickle'),'rb') as curate_file:
        curate_data = pickle.load(curate_file)
    curate_df = curate_data[['_id','text','category']].copy()
    curate_df.rename(columns={'category':'topicCategory'},inplace=True)
    return(curate_df)


def load_clin_cats_data(SUBDATAPATH):
    ct_classified = os.path.join(SUBDATAPATH,'ct_topics/')
    ct_training_files = os.listdir(ct_classified)
    ct_subtopics = pd.DataFrame(columns = ['_id','text','topicCategory'])
    for eachfile in ct_training_files:
        category = eachfile.replace('.pickle','')
        with open(os.path.join(ct_classified,eachfile),"rb") as tmpfile:
            tmpdata = pickle.load(tmpfile)
        tmpdata['topicCategory'] = category.replace('_','/')
        cleandata = tmpdata[['_id','text','topicCategory']].copy()
        ct_subtopics = pd.concat((ct_subtopics,cleandata),ignore_index=True)
    ct_subtopics.drop_duplicates(keep='first',inplace=True)
    return(ct_subtopics)

    
def load_subtopics_data(DATAPATH,RESULTSPATH,topic_dict):
    SUBDATAPATH = os.path.join(DATAPATH,'subtopics/')
    topiclist = topic_dict['broadtopics']
    litmergeddf = load_lit_subcats(DATAPATH)
    curate_df = load_citsci_data(SUBDATAPATH)
    ct_subtopics = load_clin_cats_data(SUBDATAPATH)
    allsubtopicsdf = pd.concat((ct_subtopics,curate_df,litmergeddf),ignore_index=True)
    allsubtopicsdf['topicCategory'] = allsubtopicsdf['topicCategory'].astype(str).str.replace('/','-')
    allsubtopicsdf['topicCategory'] = allsubtopicsdf['topicCategory'].astype(str).str.replace(' - ','-')
    dirty_subtopics_only = allsubtopicsdf.loc[~allsubtopicsdf['topicCategory'].isin(topiclist)]
    subtopics_only = dirty_subtopics_only.loc[dirty_subtopics_only['text'].astype(str).str.len()>20]
    training_to_export = allsubtopicsdf[['_id','topicCategory']].copy()
    training_to_export.drop_duplicates(keep='first',inplace=True)
    training_to_export['topicCategory'] = training_to_export['topicCategory'].astype(str).str.replace('-','/')
    training_to_export.to_csv(os.path.join(RESULTSPATH,'subtopicCats.tsv'),mode='w',sep='\t',header=True)
    with open(os.path.join(SUBDATAPATH,'subtopics_only.pickle'),'wb') as save_file:
        pickle.dump(subtopics_only,save_file)
    return(subtopics_only)
    
    
    