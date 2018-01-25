import subprocess
import os
import sys
import MySQLdb
from MySQLdb import Error
from datetime import datetime
from elasticsearch import Elasticsearch
import jaydebeapi
import os
import zipfile
import getopt
from common.data_connector import *
from common.util import *
from model import model
from flask import Flask, request, jsonify, redirect, url_for
from flask_cors import CORS, cross_origin
import json

data_connector=DataConnector()

app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'
app.config['supports_credentials']='true'
app.config['CORS_SUPPORTS_CREDENTIALS']='true'
cors = CORS(app)

mdl = model.Model()

@app.route("/")
def greeting():
    return "Welcome to InsightLake Speech Model Service!!"

@app.route("/train", methods=['POST'])
@cross_origin()
def trainApi():
    if request.method == 'POST':
        reqData=request.data
        jsonData = json.loads(reqData)
        data = data_connector.fetchData(jsonData)
        print(data)
        #return mdl.train(data)

    return '[]'


def trainAsync(id):
    modelInstance=data_connector.loadModelInstance(id)
    return train(id,modelInstance["model"],modelInstance["traindata"])

def train(id, model, reqdata):
    #id = instance id which we can use to update model training status
    jsonData = json.loads(reqdata)
    data = data_connector.fetchData(jsonData)
    print(data)
    response= mdl.train(data)

    if(response["status"]==2):
        mdl.persist('package/model.pickle')
        data_connector.uploadModelPackage(id,'package','package.zip',response["status"],response["reason"],response["numRecords"])
    else:
        data_connector.updateStatus(id,response["status"],response["reason"],response["numRecords"])
    #update model status

def validateAsync(id):
    modelInstance=data_connector.loadModelInstance(id)
    return train(id,modelInstance["model"],modelInstance["traindata"])

def validate(trainId, validationId, model, reqdata):
    #id = instance id which we can use to update model training status
    data_connector.downloadModelPackage(trainId,'package','package.zip')
    jsonData = json.loads(reqdata)
    data = data_connector.fetchData(jsonData)
    print(data)
    mdl.reload('package/model.pickle')
    response= mdl.validate(data)
    data_connector.updateValidationStatus(validationId,response["status"],response["reason"],json.dumps(response["report"]),response["numRecords"])
    #update model status

def predict(trainId, reqdata):
    #id = instance id which we can use to update model training status
    data_connector.downloadModelPackage(trainId,'package','package.zip')
    jsonData = json.loads(reqdata)
    data = data_connector.fetchData(jsonData)
    print(data)
    mdl.reload('package/model.pickle')
    response= mdl.predict(data)
    print(response)
    return response
    #update model status
#data=data_connector.fetch('databoard','datagov.activity')

@app.route('/predict', methods=['POST'])
@cross_origin()
def predictApi():
    if request.method == 'POST':
        doc=request.data
        return jsonify({'results': mdl.predict(doc)})
    return jsonify({'results': '[]'})


#print(data)

#update_blob(1,'package.zip')

#data_connector.loadPackage(1,'package.zip')

#loadDbData()

def usage():
    print("help text")

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ho:t:a:d:x:m:p:i:j:v", ["help", "output="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    output = None
    verbose = False
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-t", "--type"):
            type = a
        elif o in ("-a", "--action"):
            action = a
        elif o in ("-d", "--data"):
            data = a
        elif o in ("-x", "--params"):
            params = a
        elif o in ("-m", "--model"):
            model = a
        elif o in ("-p", "--port"):
            port = a
        elif o in ("-h", "--host"):
            host = a
        elif o in ("-i", "--id"):
            trainId = a
        elif o in ("-j", "--vid"):
            validationId = a
        else:
            assert False, "unhandled option"
    # ...

    if(type == "api"):
        app.run(host='0.0.0.0')
    elif(type == "function"):
        if(action == "train"):
            train(trainId,model,data)
        elif (action == "validate"):
            validate(trainId, validationId, model, data)
        elif (action == "predict"):
            predict(trainId, data)

if __name__ == "__main__":
    print(sys.argv)
    data="{\"type\": \"source\",\"location\": \"mytest\",\"store\": \"mytest.docs\"}"

    #train(57,'LogisticRegression',data)
    #validate(57,61, 'LogisticRegression', data)
    #predict(57,data)
    main()

