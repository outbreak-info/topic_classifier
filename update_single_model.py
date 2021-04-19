import os
import pandas as pd
import pathlib
from src.train_classifier import *

script_path = pathlib.Path(__file__).parent.absolute()
DATAPATH = os.path.join(script_path,'data/')
RESULTSPATH = os.path.join(script_path,'results/')
MODELPATH = os.path.join(script_path,'models/')

littopicsfile = os.path.join(DATAPATH,'litcovidtopics.tsv')
offtopicsfile = os.path.join(DATAPATH,'othertopics.tsv')
littopicsdf = read_csv(littopicsfile,delimiter='\t',header=0,index_col=0)
offtopicsdf = read_csv(offtopicsfile,delimiter='\t',header=0,index_col=0)
topicsdf = pd.concat((littopicsdf,offtopicsdf),ignore_index=True)
topiclist = topicsdf['topicCategory'].unique().tolist()    

classifiers = load_classifiers('best')
topic_to_check = input("enter the topic Category: ")
generate_models(MODELPATH,topicsdf,classifiers,topic_to_check)