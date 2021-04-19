import os
import pathlib
from src.fetch_litcovid_topics import *
    
script_path = pathlib.Path(__file__).parent.absolute()
DATAPATH = os.path.join(script_path,'data/')

get_litcovid_topics(DATAPATH)