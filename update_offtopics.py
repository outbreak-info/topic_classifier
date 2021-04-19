import os
import pathlib
from src.fetch_offtopics import *
#### MAIN
script_path = pathlib.Path(__file__).parent.absolute()
DATAPATH = os.path.join(script_path,'data/')
RESULTSPATH = os.path.join(script_path,'results/')


get_other_topics(DATAPATH,RESULTSPATH)