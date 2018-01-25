import time
import json
from kafka import KafkaConsumer, KafkaProducer
import MySQLdb
from MySQLdb import Error
from sklearn.metrics import *

class ModelBase(object):
    def __init__(self,name):
        self.name=name
        self.algo=object()
        self.properties={
            'name': 'model'
        }
        #self.producer=KafkaProducer(bootstrap_servers='localhost:9092')

    @staticmethod
    def train(method):
        def wrapper(*args,**kw):
            print("Training wrapper called")
            startTime = time.time()
            model = args[0]
            data = args[1]

            event={
                "status": "failed",
                "model": model.name,
                "user": "username",
                "startTime": startTime,
                #"dataNumRecords": len(datajson),
                "dataBytes": bytes(data),
                "endTime": 0,
                "duration": 0,
                "properties":kw
            }
            try:
                result = method(*args,**kw)
                endTime=time.time()

                event["endTime"]=endTime
                event["duration"]=(endTime-startTime)*1000
                event["status"]="success"
                return result
            except Exception as e:
                print((e))
        return ""

    @staticmethod
    def predict(method):
        def wrapper(*args, **kw):
            print("Predict wrapper called")
            startTime = time.time()
            model = args[0]
            data = args[1]

            event = {
                "status": "failed",
                "model": model.name,
                "user": "username",
                "startTime": startTime,
                # "dataNumRecords": len(datajson),
                "dataBytes": bytes(data),
                "endTime": 0,
                "duration": 0,
                "properties": kw
            }
            try:
                result = method(*args, **kw)
                endTime = time.time()

                event["endTime"] = endTime
                event["duration"] = (endTime - startTime) * 1000
                event["status"] = "success"
                return result
            except Exception as e:
                print((e))

        return ""

    @staticmethod
    def validate(method):
        def wrapper(*args, **kw):
            print("Validate wrapper called")
            startTime = time.time()
            model = args[0]
            data = args[1]

            event = {
                "status": "failed",
                "model": model.name,
                "user": "username",
                "startTime": startTime,
                # "dataNumRecords": len(datajson),
                "dataBytes": bytes(data),
                "endTime": 0,
                "duration": 0,
                "properties": kw
            }
            try:
                predictions,labels = method(*args, **kw)
                endTime = time.time()

                event["endTime"] = endTime
                event["duration"] = (endTime - startTime) * 1000
                event["status"] = "success"

                fpr,tpr,threshold=roc_curve(labels,predictions)

                roc={"fpr": fpr,"tpr": tpr,"threshold": threshold}

                report={
                    "classification_report": classification_report(labels, predictions),
                    "confusion_matrix": confusion_matrix(labels,predictions),
                    "roc": roc,
                    "roc_auc": auc(fpr,tpr)

                }
                event["validation_report"]=report
                return ""
            except Exception as e:
                print((e))

        return ""

    def loadDbData(self,event):
        connection=mysql.connector.connect(user='edpadmin',password='edpadmin',database='datagov')
        cursor=connection.cursor()

        query="select * from ml_model_events"
        cursor.execute(query)

        for (firstname, secondname) in cursor:
            print(firstname, secondname)

        cursor.close()
        connection.close()

    def logToKafka(self,event):
        self.producer.send('modelevents',event)



