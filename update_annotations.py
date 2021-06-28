import os
import pathlib

## For refreshing the litcovid annotations
from src.fetch_litcovid_topics import *
from src.fetch_offtopics import *
from src.common import topic_dict
from src.fetch_clinical_trials import *
from src.fetch_litsubtopics import *
from src.fetch_subtopics import *

## For refreshing the classification of other resources
from src.classify_pubs import *
from src.common import load_classifiers


#### MAIN
script_path = pathlib.Path(__file__).parent.absolute()
DATAPATH = os.path.join(script_path,'data/')
RESULTSPATH = os.path.join(script_path,'results/')
MODELPATH = os.path.join(script_path,'models/')
PREDICTPATH = os.path.join(script_path,'predictions/')
SUBDATAPATH = os.path.join(DATAPATH,'subtopics/')
CLINDATAPATH = os.path.join(SUBDATAPATH,'ct_topics/')

#### Refresh the litcovid and Clinical Trials annotations
get_litcovid_topics(DATAPATH)
get_other_topics(DATAPATH,RESULTSPATH)
update_clin_cats(DATAPATH,CLINDATAPATH)
get_sub_topics(DATAPATH,RESULTSPATH)
map_keywords(DATAPATH)
subtopics_only = load_subtopics_data(SUBDATAPATH,RESULTSPATH,topic_dict)

#### Refresh the classification of other resources
littopicsfile = os.path.join(DATAPATH,'litcovidtopics.tsv')
offtopicsfile = os.path.join(DATAPATH,'othertopics.tsv')
littopicsdf = read_csv(littopicsfile,delimiter='\t',header=0,index_col=0)
offtopicsdf = read_csv(offtopicsfile,delimiter='\t',header=0,index_col=0)
subtopic_results = read_csv(os.path.join(RESULTSPATH,'subtopicCats.tsv'),delimiter='\t',header=0,index_col=0)
topicsdf = pd.concat((littopicsdf,offtopicsdf,subtopic_results),ignore_index=True)
topicsdf.drop_duplicates(keep='first',inplace=True)

classifiers = load_classifiers('best')
load_annotations(DATAPATH,MODELPATH,PREDICTPATH,RESULTSPATH,topicsdf,classifiers,True)