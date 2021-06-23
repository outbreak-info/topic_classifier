import os
import requests
import json
import pandas as pd
from pandas import read_csv
import re
from collections import OrderedDict
import pickle


from src.common import *
from src.clin_mapping import *



def fetch_clin_size():
    pubmeta = requests.get("https://api.outbreak.info/resources/query?q=@type:ClinicalTrial&size=0&aggs=@type")
    pubjson = json.loads(pubmeta.text)
    pubcount = int(pubjson["facets"]["@type"]["total"])
    return(pubcount)


def get_clin_ids():
    source_size = fetch_clin_size()
    r = requests.get("https://api.outbreak.info/resources/resource/query?q=@type:ClinicalTrial&fields=_id&fetch_all=true")
    response = json.loads(r.text)
    idlist = get_ids_from_json(response)
    try:
        scroll_id = response["_scroll_id"]
        while len(idlist) < source_size:
            r2 = requests.get("https://api.outbreak.info/resources/resource/query?q=@type:ClinicalTrial&fields=_id&fetch_all=true&scroll_id="+scroll_id)
            response2 = json.loads(r2.text)
            idlist2 = set(get_ids_from_json(response2))
            tmpset = set(idlist)
            idlist = list(tmpset.union(idlist2))
            try:
                scroll_id = response2["_scroll_id"]
            except:
                print("no new scroll id")
        return(idlist)
    except:
        return(idlist)

    
def batch_fetch_clin_meta(idlist):
    ## Break the list of ids into smaller chunks so the API doesn't fail the post request
    runs = round((len(idlist))/100,0)
    i=0 
    separator = ','
    ## Create dummy dataframe to store the meta data
    textdf = pd.DataFrame(columns = ['_id','abstract','trialName','trialDescription',
                                     'designPrimaryPurpose','studyType',
                                     'interventionCategory','interventionName'])
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
        r = requests.post("https://api.outbreak.info/resources/query/", params = {'q': sample_ids, 'scopes': '_id', 'fields': 'name,abstract,description,interventions,studyDesign'})
        if r.status_code == 200:
            rawresult = json.loads(r.text)
            structuredresult = pd.json_normalize(rawresult)
            structuredresult.drop(columns=['studyDesign.@type','studyDesign.designModel',
                                           'studyDesign.phaseNumber','studyDesign.phase',
                                           'studyDesign.designAllocation','studyDesign.studyDesignText'],inplace=True)
            structuredresult.rename(columns={'name':'trialName', 'description':'trialDescription',
                                             'studyDesign.designPrimaryPurpose':'designPrimaryPurpose',
                                             'studyDesign.studyType':'studyType'},inplace=True)
            exploded = structuredresult.explode('interventions')
            no_interventions = exploded.loc[exploded['interventions'].isna()].copy()
            no_interventions_clean = no_interventions[['_id','abstract','trialName','trialDescription',
                                                       'designPrimaryPurpose','studyType']].copy()
            has_interventions = exploded.loc[~exploded['interventions'].isna()].copy()
            interventions = pd.concat([has_interventions.drop(['interventions'], axis=1), has_interventions['interventions'].apply(pd.Series)], axis=1)
            clean_interventions = interventions[['_id','abstract','trialName','trialDescription',
                                                 'designPrimaryPurpose','studyType',
                                                 'category','name']].copy()
            clean_interventions.rename(columns={'name':'interventionName','category':'interventionCategory'},inplace=True)
            textdf = pd.concat((textdf,clean_interventions,no_interventions_clean),ignore_index=True)
        i=i+1
    textdf.rename(columns={'trialName':'name','trialDescription':'description'},inplace=True)
    return(textdf)
        
    
def parse_wikidata(data):
    tmplist = []
    for item in data['results']['bindings']:
        try:
            tmplist.append(OrderedDict({
            'wdid':item['item']['value'].replace('http://www.wikidata.org/entity/',''),
            'drug_name': item['itemLabel']['value'],
            'name': item['itemLabel']['value'].lower(),
            'alias': "None"}))
            tmp= item['itemAltLabel']['value'].split(',')
            for altname in tmp:
                if len(altname.strip())>3:
                    tmplist.append(OrderedDict({
                    'wdid':item['item']['value'].replace('http://www.wikidata.org/entity/',''),
                    'drug_name': item['itemLabel']['value'],
                    'name': item['itemLabel']['value'].lower(),
                    'alias': altname.strip().lower()
                    }))
        except:
            tmplist.append(OrderedDict({
            'wdid':item['item']['value'].replace('http://www.wikidata.org/entity/',''),
            'drug_name': item['itemLabel']['value'],
            'name': item['itemLabel']['value'].lower(),
            'alias': "None"
            }))
    tmpdf = pd.DataFrame(tmplist)
    return(tmpdf)
        
    
def get_wd_drugs(): 
    repurposetypes = ['Q12140', 'Q35456', 'Q28885102','Q8386']
    url = 'https://query.wikidata.org/sparql'
    querystart = """
    SELECT
      ?item ?itemLabel ?itemAltLabel
      ?value 
    WHERE 
    {
      ?item wdt:P31 wd:"""
    queryend = """.        
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    """
    repurpose = pd.DataFrame(columns=['wdid','drug_name','name','alias'])
    for eachwdid in repurposetypes:
        query = querystart+eachwdid+queryend
        r = requests.get(url, params = {'format': 'json', 'query': query})
        data = r.json()
        tmpdf = parse_wikidata(data)
        repurpose = pd.concat((repurpose,tmpdf),ignore_index=True)
    repurpose.drop_duplicates(keep='first',inplace=True)
    return(repurpose)


def clean_drugs(clin_meta):
    drug_stopwords,general_stopwords,pharma_amounts,odd_fractions = load_drug_terms()
    drugs = clin_meta[['_id','interventionName','text']].loc[clin_meta['interventionCategory'].astype(str).str.contains('drug')].copy()
    drugs['interventionName'] = drugs['interventionName'].replace(drug_stopwords,regex=True)
    drugs['interventionName'] = drugs['interventionName'].replace(general_stopwords,regex=True)
    drugs['interventionName'] = drugs['interventionName'].str.replace(pharma_amounts,"",regex=True)
    drugs['interventionName'] = drugs['interventionName'].str.replace(odd_fractions,"",regex=True)
    drugs['interventionName'] = drugs['interventionName'].str.replace(' / ','/')
    drugs['interventionName'] = drugs['interventionName'].str.strip()
    return(drugs)


def get_repurpose(clin_meta):
    drugs = clean_drugs(clin_meta)
    drug_word_freq = drugs.groupby('interventionName').size().reset_index(name='counts')
    drug_word_freq.sort_values('counts',ascending=False,inplace=True)
    drugfreq = drug_word_freq.loc[drug_word_freq['counts']>1].copy()
    druglist = drugfreq['interventionName'].unique().tolist() 
    repurposedf = get_wd_drugs()
    all_drugs = list(set(repurposedf['name'].unique().tolist()).union(set(repurposedf['alias'].unique().tolist())))
    druglist_lower = [x.lower() for x in druglist]
    all_drugs_lower = [x.lower() for x in all_drugs]
    in_common = list(set(druglist_lower).intersection(set(all_drugs_lower)))
    missing = [x for x in druglist_lower if x not in in_common]
    repurpose_cts = drugs['_id'].loc[drugs['interventionName'].astype(str).str.lower().isin(in_common)].unique().tolist()
    return(drugs, repurpose_cts)


def dump_drug_cats(CLINDATAPATH,clin_meta):
    drugs, repurpose_cts = get_repurpose(clin_meta)
    drug_repurposing = clin_meta.loc[(clin_meta['interventionCategory']=='dietary supplement')|
                                     (clin_meta['_id'].isin(repurpose_cts))]
    pharma_cts = drugs['_id'].loc[~drugs['_id'].isin(repurpose_cts)].unique().tolist()
    pharma_info = clin_meta.loc[clin_meta['_id'].isin(pharma_cts)]
    with open(os.path.join(CLINDATAPATH,'Repurposing.pickle'),'wb') as dumpfile:
        pickle.dump(drug_repurposing,dumpfile)
    with open(os.path.join(CLINDATAPATH,'Pharmaceutical Treatments.pickle'),'wb') as dumpfile:
        pickle.dump(pharma_info,dumpfile)


def map_interventions(CLINDATAPATH,clin_meta):
    intervention_map = load_clin_dicts('intervention_map')
    for eachintervention in intervention_map.keys():
        tmpdf = clin_meta.loc[clin_meta['interventionCategory'].astype(str).str.contains(eachintervention)]
        with open(os.path.join(CLINDATAPATH,intervention_map[eachintervention]+'.pickle'),'wb') as outpath:
            pickle.dump(tmpdf,outpath)

            
def map_designpurpose(CLINDATAPATH,clin_meta):
    designpurposemap = load_clin_dicts('designpurposemap')
    for eachpurpose in designpurposemap.keys():
        tmpdf = clin_meta.loc[clin_meta['designPrimaryPurpose'].astype(str).str.contains(eachpurpose)]
        try:
            originaldf = pickle.load(open(os.path.join(CLINDATAPATH,designpurposemap[eachpurpose]+'.pickle'),'rb'))
            combidf = pd.concat((originaldf,tmpdf),ignore_index=True)
            combi.drop_duplicates(keep="first",inplace=True)
        except:
            combidf = tmpdf
        with open(os.path.join(CLINDATAPATH,designpurposemap[eachpurpose]+'.pickle'),'wb') as outpath:
            pickle.dump(combidf,outpath)


def map_diagnosis(CLINDATAPATH,clin_meta):
    diagnostickeywords = load_clin_dicts('diagnostickeywords')
    diagnosis = clin_meta.loc[((clin_meta['interventionCategory'].astype(str).str.contains('diagnostic test'))|
                          (clin_meta['designPrimaryPurpose'].astype(str).str.contains('diagnostic'))|
                          (clin_meta['designPrimaryPurpose'].astype(str).str.contains('screening')))].copy()
    for eachdiag in diagnostickeywords.keys():
        keywordlist = diagnostickeywords[eachdiag]
        topicCategory = eachdiag
        searchregex = re.compile('|'.join(keywordlist), re.IGNORECASE)
        tmpdf = diagnosis.loc[((diagnosis['interventionName'].str.contains(searchregex))|
                              (diagnosis['text'].str.contains(searchregex)))]
        with open(os.path.join(CLINDATAPATH,eachdiag.replace('/','-')+'.pickle'),'wb') as outpath:
            pickle.dump(tmpdf,outpath)  


def map_treatment(CLINDATAPATH,clin_meta):
    alltreatment = clin_meta.loc[(clin_meta['designPrimaryPurpose'].astype(str).str.contains('treatment'))]
    treatment = alltreatment.loc[((alltreatment['interventionCategory']!='drug')&
                                  (alltreatment['interventionCategory']!='biological')&
                                  (alltreatment['interventionCategory']!='genetic'))].copy()
    treatmentkeywords = load_clin_dicts('treatmentkeywords')
    for eachtreat in treatmentkeywords.keys():
        keywordlist = treatmentkeywords[eachtreat]
        topicCategory = eachtreat
        searchregex = re.compile('|'.join(keywordlist), re.IGNORECASE)
        tmpdf = treatment.loc[((treatment['interventionName'].str.contains(searchregex))|
                              (treatment['text'].str.contains(searchregex)))]
        try:
            originaldf = pickle.load(open(os.path.join(CLINDATAPATH,eachtreat+'.pickle'),'rb'))
            combidf = pd.concat((originaldf,tmpdf),ignore_index=True)
            combi.drop_duplicates(keep="first",inplace=True)
        except:
            combidf = tmpdf

        with open(os.path.join(CLINDATAPATH,eachtreat+'.pickle'),'wb') as outpath:
            pickle.dump(combidf,outpath)


def map_prevention(CLINDATAPATH,clin_meta):
    prevention = clin_meta.loc[clin_meta['designPrimaryPurpose'].astype(str).str.contains('prevention')].copy()
    preventionkeywords = load_clin_dicts('preventionkeywords')
    for eachprevent in preventionkeywords.keys():
        keywordlist = preventionkeywords[eachprevent]
        topicCategory = eachprevent
        searchregex = re.compile('|'.join(keywordlist), re.IGNORECASE)
        tmpdf = prevention.loc[((prevention['interventionName'].str.contains(searchregex))|
                                (prevention['text'].str.contains(searchregex)))]
        try:
            originaldf = pickle.load(open(os.path.join(CLINDATAPATH,eachprevent+'.pickle'),'rb'))
            combidf = pd.concat((originaldf,tmpdf),ignore_index=True)
            combidf.drop_duplicates(keep="first",inplace=True)
        except:
            combidf = tmpdf
        with open(os.path.join(CLINDATAPATH,eachprevent+'.pickle'),'wb') as outpath:
            pickle.dump(combidf,outpath)    


def apply_combi_map(CLINDATAPATH,clin_meta):
    prevention = clin_meta.loc[clin_meta['designPrimaryPurpose'].astype(str).str.contains('prevention')].copy()
    individual_prevention = prevention.loc[prevention['interventionCategory'].astype(str).str.contains('device')]
    originaldf = pickle.load(open(os.path.join(CLINDATAPATH,'Individual Prevention.pickle'),'rb'))
    combidf = pd.concat((originaldf,individual_prevention),ignore_index=True)
    combidf.drop_duplicates(keep="first",inplace=True)
    with open(os.path.join(CLINDATAPATH,'Individual Prevention.pickle'),'wb') as outpath:
        pickle.dump(combidf,outpath)


            
def update_clin_cats(DATAPATH,CLINDATAPATH):
    idlist = get_clin_ids()
    clin_meta = batch_fetch_clin_meta(idlist)
    clin_meta = merge_texts(clin_meta)
    dump_drug_cats(CLINDATAPATH,clin_meta)
    map_interventions(CLINDATAPATH,clin_meta)
    map_designpurpose(CLINDATAPATH,clin_meta)
    map_diagnosis(CLINDATAPATH,clin_meta)
    map_treatment(CLINDATAPATH,clin_meta)
    map_prevention(CLINDATAPATH,clin_meta)
    apply_combi_map(CLINDATAPATH,clin_meta)
    with open(os.path.join(DATAPATH,'clin_meta.pickle'),'wb') as outputfile:
        pickle.dump(clin_meta,outputfile)