import pandas as pd
import requests
import json
from datetime import datetime
import time

topic_dict = {'broadtopics':['Behavioral Research','Case Descriptions','Clinical',
                             'Diagnosis','Environment','Epidemiology','Forecasting',
                             'Information Sciences','Mechanism','Prevention','Risk Factors',
                             'Transmission','Treatment'],
              'subtopics':['Antibody Detection','Biologics','Host Factors','Individual Prevention',
                           'Medical Care','Pathology-Radiology','Pharmaceutical Treatments',
                           'Public Health Interventions','Rapid Diagnostics','Repurposing',
                           'Symptoms','Vaccines','Virus Detection','Prognosis',
                           'Mechanism of Infection','Mechanism of Transmission',
                           'Molecular Epidemiology','Host-Intermediate Reservoirs',
                           'Classical Epidemiology','Rapid Diagnostics','Testing Prevalence',
                           'Viral Shedding-Persistence','Immunological Response']}



#### Classifiers
def load_classifiers(classifierset_type):
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.naive_bayes import MultinomialNB
    from sklearn.neural_network import MLPClassifier
    from sklearn import tree
    from sklearn.neighbors import KNeighborsClassifier
    from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
    from sklearn.linear_model import LogisticRegression
    all_available = {
        'Random Forest':RandomForestClassifier(n_estimators=1000, random_state=0),
        'MultinomialNB':MultinomialNB(),
        'Neural Net':MLPClassifier(alpha=1, max_iter=1000),
        'Decision Tree':tree.DecisionTreeClassifier(max_depth=5),
        'Nearest Neighbor':KNeighborsClassifier(3),
        'AdaBoost':AdaBoostClassifier(),
        'Logistic Regression':LogisticRegression(random_state=0, solver='lbfgs', multi_class='ovr')}
    best = {
        'Random Forest':RandomForestClassifier(n_estimators=1000, random_state=0),
        'MultinomialNB':MultinomialNB(),
        'Logistic Regression':LogisticRegression(random_state=0, solver='lbfgs', multi_class='ovr')}
    if classifierset_type=='best':
        return(best)
    else:
        return(all_available)





#### Outbreak.info API query functions
def get_ids_from_json(jsonfile):
    idlist = []
    for eachhit in jsonfile["hits"]:
        if eachhit["_id"] not in idlist:
            idlist.append(eachhit["_id"])
    return(idlist)


#### Query by source
def fetch_src_size(source):
    pubmeta = requests.get("https://api.outbreak.info/resources/query?q=((@type:Publication) AND (curatedBy.name:"+source+"))&size=0&aggs=@type")
    pubjson = json.loads(pubmeta.text)
    pubcount = int(pubjson["facets"]["@type"]["total"])
    return(pubcount)


def get_source_ids(source):
    source_size = fetch_src_size(source)
    r = requests.get('https://api.outbreak.info/resources/query?q=((@type:Publication) AND (curatedBy.name:"'+source+'"))&fields=_id&fetch_all=true')
    response = json.loads(r.text)
    idlist = get_ids_from_json(response)
    try:
        scroll_id = response['_scroll_id']
        while len(idlist) < source_size:
            r2 = requests.get('https://api.outbreak.info/resources/query?q=((@type:Publication) AND (curatedBy.name:"'+source+'"))&fields=_id&fetch_all=true&scroll_id='+scroll_id)
            response2 = json.loads(r2.text)
            idlist2 = set(get_ids_from_json(response2))
            tmpset = set(idlist)
            idlist = tmpset.union(idlist2)
            try:
                scroll_id = response2['_scroll_id']
            except:
                print("no new scroll id")
        return(idlist)
    except:
        return(idlist)
    

def get_pub_ids(sourceset):
    pub_srcs = {"preprint":["bioRxiv","medRxiv"],"litcovid":["litcovid"],
                "other":["Figshare","Zenodo","MRC Centre for Global Infectious Disease Analysis"],
                "nonlitcovid":["Figshare","Zenodo","MRC Centre for Global Infectious Disease Analysis",
                               "bioRxiv","medRxiv"],
                "all":["Figshare","Zenodo","MRC Centre for Global Infectious Disease Analysis",
                       "bioRxiv","medRxiv","litcovid"]}
    sourcelist = pub_srcs[sourceset]
    allids = []
    for eachsource in sourcelist:
        sourceids = get_source_ids(eachsource)
        allids = list(set(allids).union(set(sourceids)))
    return(allids)


#### Query by search terms
def fetch_query_size(query):
    pubmeta = requests.get('https://api.outbreak.info/resources/query?q=(("'+query+'") AND (@type:Publication))&size=0&aggs=@type')
    pubjson = json.loads(pubmeta.text)
    pubcount = int(pubjson["facets"]["@type"]["total"])
    return(pubcount)


def get_query_ids(query):
    query_size = fetch_query_size(query)
    r = requests.get('https://api.outbreak.info/resources/query?q=(("'+query+'") AND (@type:Publication))&fields=_id&fetch_all=true')
    response = json.loads(r.text)
    if "error" not in response:
        idlist = get_ids_from_json(response)
    try:
        scroll_id = response["_scroll_id"]
        while len(idlist) < query_size:
            r2 = requests.get('https://api.outbreak.info/resources/query?q=(("'+query+'") AND (@type:Publication))&fields=_id&fetch_all=true&scroll_id='+scroll_id)
            response2 = json.loads(r2.text)
            if "error" not in response2:
                idlist2 = set(get_ids_from_json(response2))
                tmpset = set(idlist)
                idlist = tmpset.union(idlist2)
                try:
                    scroll_id = response2["_scroll_id"]
                except:
                    print("no new scroll id")
        return(idlist)
    except:
        return(idlist)


#### Retrieve metadata
def batch_fetch_meta(idlist):
    ## Break the list of ids into smaller chunks so the API doesn't fail the post request
    runs = round((len(idlist))/100,0)
    i=0 
    separator = ','
    ## Create dummy dataframe to store the meta data
    textdf = pd.DataFrame(columns = ['_id','abstract','name','description'])
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
        r = requests.post("https://api.outbreak.info/resources/query/", params = {'q': sample_ids, 'scopes': '_id', 'fields': 'name,abstract,description'})
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
            cleanresult = rawresult[['_id','name','abstract','description']].loc[rawresult['_score']==1].fillna(" ").copy()
            cleanresult.drop_duplicates(subset='_id',keep="first", inplace=True)
            textdf = pd.concat((textdf,cleanresult))
        i=i+1
    return(textdf)

 
def batch_fetch_keywords(idlist):
    ## Break the list of ids into smaller chunks so the API doesn't fail the post request
    runs = round((len(idlist))/100,0)
    i=0 
    separator = ','
    ## Create dummy dataframe to store the meta data
    textdf = pd.DataFrame(columns = ['_id','abstract','name','keywords'])
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
        r = requests.post("https://api.outbreak.info/resources/query/", params = {'q': sample_ids, 'scopes': '_id', 'fields': 'name,abstract,keywords'})
        if r.status_code == 200:
            rawresult = pd.read_json(r.text)
            checkcols = rawresult.columns
            if (('keywords' not in checkcols) and ('abstract' in checkcols)):
                rawresult['keywords']=[""]
            elif (('keywords' in checkcols) and ('abstract' not in checkcols)):
                rawresult['abstract']=" "
            elif (('keywords' not in checkcols) and ('abstract' not in checkcols)):
                rawresult['abstract']=" "
                rawresult['keywords']=[""]
            cleanresult = rawresult[['_id','name','abstract','keywords']].loc[rawresult['_score']==1].fillna(" ").copy()
            cleanresult.drop_duplicates(subset='_id',keep="first", inplace=True)
            textdf = pd.concat((textdf,cleanresult))
        i=i+1
    return(textdf)



#### Formatting functions
def clean_results(allresults):
    allresults.drop_duplicates(keep='first',inplace=True)
    cleanresults = allresults.groupby('_id')['topicCategory'].apply(list).reset_index(name='newTopicCategory')
    cleanresults.rename(columns={'newTopicCategory':'topicCategory'},inplace=True)
    return(cleanresults) 


def merge_texts(df):
    df.fillna('',inplace=True)
    df['text'] = df['name'].astype(str).str.cat(df['abstract'].astype(str).str.cat(df['description'],sep=' '),sep=' ')
    df['text'] = df['text'].str.replace(r'\W', ' ')
    df['text'] = df['text'].str.replace(r'\s+[a-zA-Z]\s+', ' ')
    df['text'] = df['text'].str.replace(r'\^[a-zA-Z]\s+', ' ')
    df['text'] = df['text'].str.lower()   
    return(df)





def get_delay(date):
    try:
        date = datetime.strptime(date, '%a, %d %b %Y %H:%M:%S GMT')
        timeout = int((date - datetime.now()).total_seconds())
    except ValueError:
        timeout = int(date)
    return timeout



def make_request(params):
    wikidata_url = 'https://query.wikidata.org/sparql'
    r = requests.get(wikidata_url, params)
    if r.status_code == 200:
        return r
    if r.status_code == 500:
        return 0
    if r.status_code == 403:
        return 0
    if r.status_code == 429:
        timeout = get_delay(r.headers['retry-after'])
        print('Timeout {} m {} s'.format(timeout // 60, timeout % 60))
        time.sleep(timeout)
        make_request(params)


