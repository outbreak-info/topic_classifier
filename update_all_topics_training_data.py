import os
import pathlib
from src.fetch_litcovid_topics import *
from src.fetch_offtopics import *
from src.common import topic_dict

script_path = pathlib.Path(__file__).parent.absolute()
DATAPATH = os.path.join(script_path,'data/')
RESULTSPATH = os.path.join(script_path,'results/')
SUBDATAPATH = os.path.join(DATAPATH,'subtopics/')
CLINDATAPATH = os.path.join(SUBDATAPATH,'ct_topics/')

print('fetching litcovid topics')
get_litcovid_topics(DATAPATH)
print('updating other broad topics in litcovid')
get_other_topics(DATAPATH,RESULTSPATH)

print('updating clinical trials annotations')
from src.fetch_clinical_trials import *
update_clin_cats(DATAPATH,CLINDATAPATH)

print('fetching subtopics from litcovid via keyword-mapping')      
from src.fetch_litsubtopics import *
get_sub_topics(DATAPATH,RESULTSPATH)
map_keywords(DATAPATH)

print('fetching all available subtopic data')
from src.fetch_subtopics import *
subtopics_only = load_subtopics_data(SUBDATAPATH,RESULTSPATH,topic_dict)