import gensim
import numpy
import pickle
from common.model_base import *
import json
from gensim import corpora, similarities, models
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import LabelBinarizer
from sklearn.linear_model import LogisticRegression

try:
    print("Model initialization")

except:
    pass

def analyzer_(tokens):
    return set(token for token in tokens if len(token)>2)

class Model(ModelBase):
    def __init__(self):
        super(Model,self).__init__("LR")
        self.vectorizer=CountVectorizer(lowercase=False,binary=True, analyzer=analyzer_)
        self.binarizer=LabelBinarizer()
        self.algo = LogisticRegression(C=1,fit_intercept=False,class_weight='balanced')
        self.label_dict={}
        self.label_index=0
        self.label_map={}



    def process_text(self, text):
        words = text.lower().split()
        stopwords = set('for a an the of and to in'.split())
        words=[word for word in words if word not in stopwords]
        return words

    def hot_encode(self,word):
        index=self.label_index
        if(word[0] not in self.label_dict):
            self.label_dict[word[0]]=self.label_index
            self.label_map[self.label_index]=word[0]
            index=self.label_index
            self.label_index+=1
        else:
            index=self.label_dict[word[0]]
        return index

    #@ModelBase.train
    def train(self,docs):
        # Create sentences from documents
        #docs=json.loads(data)

        inputs=[]
        labels=[]

        for doc in docs:
            inputs.append(self.process_text(doc['rawText']))
            labels.append(self.hot_encode(doc['tags']))

        self.binarizer.fit_transform(labels)


        inputs = self.vectorizer.fit_transform(inputs)
        print(inputs)
        print(labels)
        self.vectorizer.stop_words_=None

        self.algo.fit(inputs, labels)

        print('Training done')
        return {'status': 2, 'reason': '', 'numRecords': len(docs)}

    def validate(self,docs):
        #docs = json.loads(data)
        inputs = []
        labels = []

        for doc in docs:
            inputs.append(self.process_text(doc['rawText']))
            labels.append(self.hot_encode(doc['tags']))

        inputs = self.vectorizer.transform(inputs)
        predictions = self.algo.predict(inputs)
        fpr, tpr, threshold = roc_curve(labels, predictions)

        roc = {"fpr": fpr.tolist(), "tpr": tpr.tolist(), "threshold": threshold.tolist()}

        report = {
            "classification_report": classification_report(labels, predictions),
            "confusion_matrix": confusion_matrix(labels, predictions).tolist(),
            "roc": roc,
            "roc_auc": auc(fpr, tpr)

        }
        print (report)
        return {'status': 2, 'reason':'', 'report': report, 'numRecords': len(docs)}

    def predict(self,docs):
        #docs = json.loads(data)

        inputs = [self.process_text(element['rawText']) for element in docs]
        inputs = self.vectorizer.transform(inputs)

        predictions = self.algo.predict(inputs)

        results=[]
        index=0
        for i in predictions.tolist():
            result={}
            result["predicted_tags"]=self.label_map[i]
            results.append(result)
            index+=1

        print (results)

        return results

    def persist(self, path):
        package={
            'vectorizer': self.vectorizer,
            'binarizer': self.binarizer,
            'algo': self.algo,
            'label_map': self.label_map,
            'label_dict': self.label_dict
        }
        with open(path, 'wb') as file:
            pickle.dump(package,file, -1)

    def reload(self, path):

        with open(path, 'rb') as file:
            package = pickle.load(file)

        for key,value in package.items():
            setattr(self,key,value)

if __name__ == '__main__':
    m=Model()

    docs = [{"rawText":"Apple is good phone company", "tags": "tech"},
            {"rawText": "Google search is used everywhere", "tags": "consumer"},
            {"rawText": "GE makes plane parts", "tags": "consumer"},
            {"rawText": "Facebook social graph is very big", "tags": "social"},
            {"rawText": "Twitter user growth is slowing down", "tags": "social"},
            {"rawText": "Boeing planes are selling fast", "tags": "consumer"},
            {"rawText": "Android phones are very open", "tags": "tech"},
            {"rawText": "Big data intelligence will deliver better results", "tags": "tech"},
            {"rawText": "Social sites will post lot of messages", "tags": "social"},
            {"rawText": "Tesla electric cars will be very popular", "tags": "consumer"}]
    m.train(docs)
    m.persist('package/model.pickle')
    m.predict('[{"rawText":"Electric cars are selling fast"}]')



