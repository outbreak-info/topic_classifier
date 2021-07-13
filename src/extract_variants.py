#### Load all litcovid into a dataframe, and clean up the text for searching
import os
import re
import json
import requests
import pandas as pd
from pandas import read_csv
from datetime import datetime
from collections import OrderedDict
import pickle
import time
from src.common import *


def batch_fetch_dated_meta(idlist):
    ## Break the list of ids into smaller chunks so the API doesn't fail the post request
    runs = round((len(idlist))/100,0)
    i=0 
    separator = ','
    ## Create dummy dataframe to store the meta data
    textdf = pd.DataFrame(columns = ['_id','abstract','name','description','date'])
    while i < runs+1:
        if len(idlist)<100:
            sample = idlist
        elif i == 0:
            sample = idlist[i:(i+1)*100]
        elif i == runs:
            sample = idlist[i*100:len(idlist)]
        else:
            sample = idlist[i*100:(i+1)*100]
        sample_ids = separator.join(sample)
        ## Get the text-based metadata (abstract, title) and save it
        r = requests.post("https://api.outbreak.info/resources/query/", params = {'q': sample_ids, 'scopes': '_id', 'fields': 'name,abstract,description,date'})
        if r.status_code == 200:
            rawresult = pd.read_json(r.text)
            checkcols = rawresult.columns
            if (('description' not in checkcols) and ('abstract' in checkcols)):
                rawresult['description']=" "
            elif (('description' in checkcols) and ('abstract' not in checkcols)):
                rawresult['abstract']=" "
            elif (('description' not in checkcols) and ('abstract' not in checkcols)):
                rawresult['abstract']=" "
                rawresult['description']=" "
            cleanresult = rawresult[['_id','name','abstract','description','date']].loc[rawresult['_score']==1].fillna(" ").copy()
            cleanresult.drop_duplicates(subset='_id',keep="first", inplace=True)
            textdf = pd.concat((textdf,cleanresult))
        i=i+1
        time.sleep(1)
    return(textdf)



def dirty_merge_texts(df):
    df.fillna('',inplace=True)
    df['text'] = df['name'].astype(str).str.cat(df['abstract'].astype(str).str.cat(df['description'],sep=' '),sep=' ')
    return(df)


def clean_texts(df):
    df.fillna('',inplace=True)
    df['cleantext'] = df['text']
    df['cleantext'] = df['cleantext'].str.replace(r'\W', ' ')
    df['cleantext'] = df['cleantext'].str.replace(r'\s+[a-zA-Z]\s+', ' ')
    df['cleantext'] = df['cleantext'].str.replace(r'\^[a-zA-Z]\s+', ' ')
    df['cleantext'] = df['cleantext'].str.lower()   
    return(df)



token_dict = {
    'mutants':r'\b((?:A|C|D|E|F|G|H|I|K|L|M|N|P|Q|R|S|T|V|W|Y)\d{2,5}(?:A|C|D|E|F|G|H|I|K|L|M|N|P|Q|R|S|T|V|W|Y))\b',
    'genemute':r"\b((?:ORF1a|ORF1b|S|Spike|spike|ORF3a|ORF3b|E|Envelope|envelope|M|M protein|M\(pro\)|ORF6|ORF7a|ORF7b|ORF8|ORF9b|ORF10|ORF14|3'UTR|3UTR|(?:(?:NSP|nsp|Nsp|N)(?:1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16)))(?:\s|:)(?:A|C|D|E|F|G|H|I|K|L|M|N|P|Q|R|S|T|V|W|Y)\d{1,5}(?:A|C|D|E|F|G|H|I|K|L|M|N|P|Q|R|S|T|V|W|Y))\b",
    'deletions':r"\b((?:ORF1a|ORF1b|S|Spike|spike|ORF3a|ORF3b|E|Envelope|envelope|M|M protein|M\(pro\)|ORF6|ORF7a|ORF7b|ORF8|ORF9b|ORF10|ORF14|3'UTR|3UTR|(?:(?:NSP|nsp|Nsp|N)(?:1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16)))(?:∆|(?:DEL|Del|del|:DEL|:Del|:del))\d{1,5})\b",
    'nonspec_deletion':r"\b((?:ORF1a|ORF1b|S|Spike|spike|ORF3a|ORF3b|E|Envelope|envelope|M|M protein|M\(pro\)|ORF6|ORF7a|ORF7b|ORF8|N|ORF9b|ORF10|ORF14|3'UTR|3UTR|(?:NSP|Nsp|nsp)(?:1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16))\s(?:(?:\d{1,5})(?:\/)(?:\d{1,5})|(?:\d{1,5}))(?:\s|-)(?:deletion))\b"
}

def lowerlist(x):
    lowerlist = []
    for y in x:
        entry = y.lower()
        lowerlist.append(entry)
    cleanlist = list(set(lowerlist))
    return(cleanlist)


def extract_mutations(RESULTSPATH, textdf, token_dict, export=True):
    mutationslist = pd.DataFrame(columns=['_id','name','abstract','description','text','date','mutations'])
    geneprots = r"\b(?:ORF1a|ORF1b|Spike|spike|ORF3a|ORF3b|Envelope|envelope|M protein|M\(pro\)|ORF6|ORF7a|ORF7b|ORF8|ORF9b|ORF10|ORF14|3'UTR|3UTR|(?:(?:NSP|nsp|Nsp|N)(?:1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16)))\b"
    for eachkey in token_dict.keys():
        tmpdf = textdf.loc[textdf['text'].str.contains(token_dict[eachkey])].copy()
        tmpdf['mutations'] = tmpdf['text'].str.findall(token_dict[eachkey])
        tmpmutationslist = tmpdf.explode('mutations').copy()
        mutationslist = pd.concat((mutationslist,tmpmutationslist),ignore_index=True)
    mutationslist['date'] = pd.to_datetime(mutationslist['date'])
    mutationslist.drop_duplicates(keep='first',inplace=True)
    mutationslist['gene_mentions'] = mutationslist['text'].str.findall(geneprots)
    mutationslist['gene_mentions'] = mutationslist['gene_mentions'].apply(lambda x: lowerlist(x)) 
    mutationsclean = mutationslist[['_id','name','date','mutations','gene_mentions']].copy()
    humefactors = mutationslist.loc[mutationslist['text'].str.contains("polymorphism")].copy()
    humefactors.drop_duplicates(keep="first",inplace=True)
    humefactors.drop(columns=['abstract','text','description'],inplace=True)
    if export==True:
        mutationsclean.to_csv(os.path.join(RESULTSPATH,'mutations.tsv'),sep='\t',header=True)
        humefactors.to_csv(os.path.join(RESULTSPATH,'polymorphisms.tsv'),sep='\t',header=True)
    else:
        return(mutationsclean,humefactors)
    



## Fetch lineages from Wikidata
lineagequerylist = [
    """
    SELECT
      ?item ?itemLabel ?itemAltLabel
    WHERE 
    {
      ?item wdt:P31 wd:Q104450895.        
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }""", 
    """
    SELECT 
      ?item ?itemLabel ?itemAltLabel
    WHERE 
    {
      ?item wdt:P279 wd:Q82069695.
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    """,
    """
    SELECT 
      ?item ?itemLabel ?itemAltLabel
    WHERE 
    {
      ?item wdt:P31 wd:Q105758262.
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    """    
]

def variant_names(DATAPATH,lineagequerylist): 
    WIKIDATAPATH = os.path.join(DATAPATH,'from wikidata/')
    url = 'https://query.wikidata.org/sparql'
    headers = {'User-Agent': 'outbreak variant extraction bot (https://outbreak.info/; help@outbreak.info)'}
    variants = []
    i=0
    for i in range(len(lineagequerylist)):
        query = lineagequerylist[i]
        params = {'format': 'json', 'query': query, 'headers':headers}
        r = make_request(params)
        if r != 0 and r != None:
            data = r.json()
            with open(os.path.join(WIKIDATAPATH,str(i)+'.pickle'),'wb') as dumpfile:
                pickle.dump(data,dumpfile)
        else:
            with open(os.path.join(WIKIDATAPATH,str(i)+'.pickle'),'rb') as loadfile:
                data = pickle.load(loadfile)
        for item in data['results']['bindings']:
            try:
                variants.append(OrderedDict({
                'name': item['itemLabel']['value'],
                'alias': item['itemLabel']['value']}))
                tmp= item['itemAltLabel']['value'].split(',')
                for altname in tmp:
                    if len(altname.strip())>3:
                        variants.append(OrderedDict({
                        'name': item['itemLabel']['value'],
                        'alias': altname
                        }))
            except:
                variants.append(OrderedDict({
                'name': item['itemLabel']['value'].strip(),
                'alias': item['itemLabel']['value'].strip()
                }))
        i=i+1
        time.sleep(1)
    wikivariants = pd.DataFrame(variants)
    wikivariants.drop_duplicates(keep='first',inplace=True)
    return(wikivariants)



## Generic search terms for filtering
filter_terms = ["variant","voi","voc","mutant","mutation","lineage","strain","species","clade","branch"]


def get_pango_lineages():
    lineagetable = read_csv("https://raw.githubusercontent.com/cov-lineages/pango-designation/master/lineages.csv",error_bad_lines=False,header=0)
    lineages = lineagetable['lineage'].loc[lineagetable['lineage'].str.len()>2].unique().tolist() 
    return(lineages)


def get_wiki_variants(DATAPATH,wikivariants):
    wikidict = {}
    i=0
    while i < len(wikivariants):
        wikidict[wikivariants.iloc[i]['alias'].lower().strip()] = wikivariants.iloc[i]['name'].lower().strip()
        i=i+1
    return(wikidict)


def extract_lineages(DATAPATH,RESULTSPATH, lineagequerylist, textdf, export=True):
    lineages = get_pango_lineages()
    wikivariants = variant_names(DATAPATH,lineagequerylist)
    wikidict = get_wiki_variants(DATAPATH,wikivariants)
    masterlist = list(set(lineages).union(set(wikivariants['alias'].tolist())))
    regexlist = []
    for eachitem in masterlist:
        searchstring = rf"{re.escape(eachitem)}"
        regexlist.append(searchstring)
    searchregex = re.compile('|'.join(regexlist), re.IGNORECASE)
    lineagedf = textdf.loc[textdf['text'].str.contains(searchregex)].copy()
    lineagedf['lineages'] = lineagedf['text'].str.findall(searchregex)
    rawlineageslist = lineagedf.explode('lineages').copy()
    cleanlineageslist = rawlineageslist[['_id','name','date','lineages']].copy()
    cleanlineageslist['lineages'] = [x.strip().lower() for x in cleanlineageslist['lineages']]
    cleanlineageslist['lineages'].replace(wikidict,inplace=True)
    cleanlineageslist.drop_duplicates(keep='first',inplace=True)
    if export==True:
        cleanlineageslist.to_csv(os.path.join(RESULTSPATH,'lineages.tsv'),sep='\t',header=True)
    else:
        return(cleanlineageslist)