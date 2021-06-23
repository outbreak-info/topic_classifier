import os
import pathlib
import pandas as pd
from pandas import read_csv


#### MAIN
script_path = pathlib.Path(__file__).parent.absolute()
DATAPATH = os.path.join(script_path,'data/')
RESULTSPATH = os.path.join(script_path,'results/')
SUBDATAPATH = os.path.join(DATAPATH,'subtopics/')
CLINDATAPATH = os.path.join(SUBDATAPATH,'ct_topics/')


from src.fetch_clinical_trials import *
update_clin_cats(DATAPATH,CLINDATAPATH)

from src.fetch_litsubtopics import *
from src.fetch_litcovid_topics import *
get_sub_topics(DATAPATH,RESULTSPATH)
map_keywords(DATAPATH)

from src.fetch_subtopics import *
from src.common import topic_dict
subtopics_only = load_subtopics_data(SUBDATAPATH,RESULTSPATH,topic_dict)