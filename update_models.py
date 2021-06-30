import os
import pandas as pd
import pathlib
from src.train_classifier import *
from src.fetch_subtopics import *
from src.common import topic_dict

script_path = pathlib.Path(__file__).parent.absolute()
DATAPATH = os.path.join(script_path,'data/')
RESULTSPATH = os.path.join(script_path,'results/')
MODELPATH = os.path.join(script_path,'models/')
SUBMODELPATH = os.path.join(MODELPATH,'subtopics/')
SUBDATAPATH = os.path.join(DATAPATH,'subtopics/')

littopicsfile = os.path.join(DATAPATH,'litcovidtopics.tsv')
offtopicsfile = os.path.join(DATAPATH,'othertopics.tsv')
littopicsdf = read_csv(littopicsfile,delimiter='\t',header=0,index_col=0)
offtopicsdf = read_csv(offtopicsfile,delimiter='\t',header=0,index_col=0)

classifiers = load_classifiers('best')

models_to_update = 'i'
while models_to_update not in ['b','a','s','c']:
    models_to_update = input("Which models need to be updated? (b: broad topics, c: child/sub-topics, a: all topics, s: single topic")

if models_to_update == 'a':
    topicsdf = pd.concat((littopicsdf,offtopicsdf),ignore_index=True)
    topicsdf.dropna(axis=0,inplace=True)
    topiclist = topicsdf['topicCategory'].unique().tolist()
    generate_models(MODELPATH,topicsdf,classifiers) 
    subtopics_only = load_subtopics_data(DATAPATH,RESULTSPATH,topic_dict)
    generate_models(SUBMODELPATH,subtopics_only,classifiers,"all",False)
elif models_to_update == 'b':
    topicsdf = pd.concat((littopicsdf,offtopicsdf),ignore_index=True)
    topicsdf.dropna(axis=0,inplace=True)
    topiclist = topicsdf['topicCategory'].unique().tolist()
    generate_models(MODELPATH,topicsdf,classifiers)
elif models_to_update == 'c':
    subtopics_only = load_subtopics_data(DATAPATH,RESULTSPATH,topic_dict)
    subtopics_only.dropna(axis=0,inplace=True)
    generate_models(SUBMODELPATH,subtopics_only,classifiers,"all",False)
elif models_to_update == 's':
    topic_to_check = input("enter the topic Category: ")
    if topic_to_check in topic_dict['broadtopics']:
        topicsdf = pd.concat((littopicsdf,offtopicsdf),ignore_index=True)
        topicsdf.dropna(axis=0,inplace=True)
        topiclist = topicsdf['topicCategory'].unique().tolist()
        generate_models(MODELPATH,topicsdf,classifiers,topic_to_check)
    else:
        subtopics_only = load_subtopics_data(DATAPATH,RESULTSPATH,topic_dict)
        subtopics_only.dropna(axis=0,inplace=True)
        generate_models(SUBMODELPATH,subtopics_only,classifiers,topic_to_check,False)        