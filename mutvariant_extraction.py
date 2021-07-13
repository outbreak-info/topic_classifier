import os
import re
import requests
import json
import pathlib
import pandas as pd
from pandas import read_csv
from datetime import datetime
from src.common import *
from src.extract_variants import *

script_path = pathlib.Path(__file__).parent.absolute()
DATAPATH = os.path.join(script_path,'data/')
RESULTSPATH = os.path.join(script_path,'results/')

allids = get_pub_ids('all')
metadf = batch_fetch_dated_meta(allids)
textdf = merge_texts(metadf)
mutationsclean = extract_mutations(RESULTSPATH, textdf, token_dict, export=True)
cleanlineageslist = extract_lineages(DATAPATH, RESULTSPATH, lineagequerylist, textdf, export=True)