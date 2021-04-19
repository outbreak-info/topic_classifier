import pandas as pd
import requests

def get_ids_from_json(jsonfile):
    idlist = []
    for eachhit in jsonfile["hits"]:
        if eachhit["_id"] not in idlist:
            idlist.append(eachhit["_id"])
    return(idlist)


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

    

def clean_results(allresults):
    allresults.drop_duplicates(keep="first",inplace=True)
    counts = allresults.groupby('_id').size().reset_index(name='counts')
    duplicates = counts.loc[counts['counts']>1]
    singles = counts.loc[counts['counts']==1]
    dupids = duplicates['_id'].unique().tolist()
    tmplist = []
    for eachid in dupids:
        catlist = allresults['topicCategory'].loc[allresults['_id']==eachid].tolist()
        tmplist.append({'_id':eachid,'topicCategory':catlist})
    tmpdf = pd.DataFrame(tmplist)  
    tmpsingledf = allresults[['_id','topicCategory']].loc[allresults['_id'].isin(singles['_id'].tolist())]
    idlist = tmpsingledf['_id'].tolist()
    catlist = tmpsingledf['topicCategory'].tolist()
    cattycat = [[x] for x in catlist]
    list_of_tuples = list(zip(idlist,cattycat))
    singledf = pd.DataFrame(list_of_tuples, columns = ['_id', 'topicCategory']) 
    cleanresults = pd.concat((tmpdf,singledf),ignore_index=True)
    return(cleanresults) 


def merge_texts(df):
    df.fillna('',inplace=True)
    df['text'] = df['name'].astype(str).str.cat(df['abstract'].astype(str).str.cat(df['description'],sep=' '),sep=' ')
    df['text'] = df['text'].str.replace(r'\W', ' ')
    df['text'] = df['text'].str.replace(r'\s+[a-zA-Z]\s+', ' ')
    df['text'] = df['text'].str.replace(r'\^[a-zA-Z]\s+', ' ')
    df['text'] = df['text'].str.lower()   
    return(df)

