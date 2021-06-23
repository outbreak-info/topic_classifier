import os
import requests
import pandas as pd
from pandas import read_csv
import time
from datetime import datetime
import json
import pickle
import sklearn
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, roc_auc_score


#### Import local scripts
from src.common import load_classifiers
from src.common import batch_fetch_meta
from src.common import merge_texts


#### Merge text from the name, abstract, and description
#### Clean up up the text


def fetch_categorized_data(df):
    alldata = pd.DataFrame(columns=['_id','name','abstract','description','text','topicCategory'])
    breakdown = df.groupby('topicCategory').size().reset_index(name='counts')
    for eachtopic in breakdown['topicCategory'].tolist():
        tmpids = df['_id'].loc[df['topicCategory']==eachtopic]
        tmptxtdf = batch_fetch_meta(tmpids)
        tmptxtdf = merge_texts(tmptxtdf)
        tmptxtdf['topicCategory']=eachtopic
        alldata = pd.concat((alldata,tmptxtdf),ignore_index=True)
    return(alldata)


def generate_training_df(df,category):
    tmpdf = df.loc[df['topicCategory']==category]
    positiveids = tmpdf['_id'].tolist()
    training_set_pos = df[['_id','text']].loc[df['topicCategory']==category].copy()
    training_set_pos['target']='in category'
    max_negs = len(positiveids)
    if len(positiveids)<len(df.loc[~df['_id'].isin(positiveids)]):
        training_set_neg = df[['_id','text']].loc[~df['_id'].isin(positiveids)].sample(n=max_negs).copy()
    else:
        training_set_neg = df[['_id','text']].loc[~df['_id'].isin(positiveids)].copy()
    training_set_neg['target']='not in category'
    training_set = pd.concat((training_set_pos,training_set_neg),ignore_index=True)
    return(training_set)


def train_test_classify(classifier,training_set,X,i=0):
    X_train, X_test, y_train, y_test = train_test_split(X, training_set.target, test_size=0.2, random_state=i)
    classifier.fit(X_train, y_train)
    y_pred = classifier.predict(X_test)
    cmresult = confusion_matrix(y_test,y_pred)
    report = pd.DataFrame(classification_report(y_test,y_pred,output_dict=True))
    probs = classifier.predict_proba(X_test)
    probs = probs[:, 1]
    auc = roc_auc_score(y_test, probs)
    return(cmresult,report,auc)


def vectorize_text(training_set):
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(training_set['text'])
    features = vectorizer.get_feature_names()
    return(X)


def generate_vectorizer(MODELPATH,training_set,category):
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(training_set['text'])
    features = vectorizer.get_feature_names()
    vectorizerfile = os.path.join(MODELPATH,"vectorizer_"+category+".pickle")
    xfile = os.path.join(MODELPATH,"X_"+category+".pickle")
    pickle.dump(vectorizer, open(vectorizerfile, "wb"))
    pickle.dump(X, open(xfile, "wb"))
    return(X)


def save_model(MODELPATH,classifier,classname,category):
    filename = os.path.join(MODELPATH,classname+"_"+category+".sav")
    pickle.dump(classifier, open(filename, 'wb'))

    
    
def run_test(RESULTPATH,topicsdf,classifierset_type='best',export_report=False):
    classifiers = load_classifiers(classifierset_type)
    fetchstarttime = datetime.now()
    print("fetching the abstracts: ", fetchstarttime)
    alldata = fetch_categorized_data(topicsdf)
    fetchtime = datetime.now()-fetchstarttime
    print("fetching complete: ",fetchtime)
    breakdown = alldata.groupby('topicCategory').size().reset_index(name='counts')
    testresults = []
    for eachtopic in breakdown['topicCategory'].tolist():
        print("now testing: ",eachtopic,datetime.now())
        training_set = generate_training_df(alldata,eachtopic)
        X = vectorize_text(training_set)
        for classifier in classifiers.keys():
            i=0
            while i<5:
                timestart = datetime.now()
                cmresult,report,auc = train_test_classify(classifiers[classifier],training_set,X,i)
                runtime = datetime.now() - timestart
                testresults.append({'topicCategory':eachtopic,'set size':len(training_set),'classifier':classifier,
                                    'runtime':runtime,'auc':auc,'report':report,'matrix':cmresult,'i':i})
                i=i+1
    testresultsdf = pd.DataFrame(testresults)
    if export_report==True:
        testresultsdf.to_csv(os.path.join(RESULTPATH,'in_depth_classifier_test.tsv'),sep='\t',header=True)
    return(testresultsdf)


#### Use default to generate new models on ALL available topics.
#### Otherwise, set 'traintopics' to a specific topicCategory to create a model just on that topicCategory
def generate_models(MODELPATH,topicsdf,classifiers,traintopics="all",fetch_data=True):
    if fetch_data == True:
        rawdata = fetch_categorized_data(topicsdf)
        alldata = rawdata.loc[rawdata['text'].astype(str).str.len()>3]
    else:
        alldata = topicsdf.loc[topicsdf['text'].astype(str).str.len()>3]
    breakdown = alldata.groupby('topicCategory').size().reset_index(name='counts')
    if traintopics != "all":
        eachtopic = traintopics
        trainingset = generate_training_df(alldata,eachtopic)
        X = generate_vectorizer(MODELPATH,trainingset,eachtopic)
        for eachclassifier in classifiers.keys():
            classifier=classifiers[eachclassifier]
            classifier.fit(X, trainingset.target)
            save_model(MODELPATH,classifier,eachclassifier,eachtopic)     
    else:
        for eachtopic in breakdown['topicCategory'].tolist():
            trainingset = generate_training_df(alldata,eachtopic)
            X = generate_vectorizer(MODELPATH,trainingset,eachtopic)
            for eachclassifier in classifiers.keys():
                classifier=classifiers[eachclassifier]
                classifier.fit(X, trainingset.target)
                save_model(MODELPATH,classifier,eachclassifier,eachtopic)    
