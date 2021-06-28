import os
import pathlib

from src.classify_pubs import *
from src.common import load_classifiers
#### MAIN
script_path = pathlib.Path(__file__).parent.absolute()
DATAPATH = os.path.join(script_path,'data/')
RESULTSPATH = os.path.join(script_path,'results/')
MODELPATH = os.path.join(script_path,'models/')
PREDICTPATH = os.path.join(script_path,'predictions/')

littopicsfile = os.path.join(DATAPATH,'litcovidtopics.tsv')
offtopicsfile = os.path.join(DATAPATH,'othertopics.tsv')
littopicsdf = read_csv(littopicsfile,delimiter='\t',header=0,index_col=0)
offtopicsdf = read_csv(offtopicsfile,delimiter='\t',header=0,index_col=0)
subtopic_results = read_csv(os.path.join(RESULTSPATH,'subtopicCats.tsv'),delimiter='\t',header=0,index_col=0)
topicsdf = pd.concat((littopicsdf,offtopicsdf,subtopic_results),ignore_index=True)
topicsdf.drop_duplicates(keep='first',inplace=True)

classifiers = load_classifiers('best')
load_annotations(DATAPATH,MODELPATH,PREDICTPATH,RESULTSPATH,topicsdf,classifiers,False)
