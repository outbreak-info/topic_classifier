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
   

classifiers = load_classifiers('best')
subtopics_only = load_subtopics_data(SUBDATAPATH,RESULTSPATH,topic_dict)
generate_models(SUBMODELPATH,subtopics_only,classifiers,"all",False)