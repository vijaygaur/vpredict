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
from common.util import *
import json

class DataConnector:
    def __init__(self):
        self.connection = MySQLdb.connect(host="edp.c7uqx6xqd0nl.us-east-1.rds.amazonaws.com", user="edpadmin", passwd="edpadmin",
                             db="mytest")

    def getLocation(self, location):
        cursor = self.connection.cursor()


        query = "select name,type,host,url,user,password,schemaname,driver,access_key,secret_key,port,bucket,db from datagov.data_location where name='"+location+"'"
        cursor.execute(query)
        results = cursor.fetchall()

        dataLocation={}
        for row in results:
            dataLocation["name"]=row[0]
            dataLocation["type"] = row[1]
            dataLocation["host"] = row[2]
            dataLocation["url"] = row[3]
            dataLocation["user"] = row[4]
            dataLocation["password"] = row[5]
            dataLocation["schemaname"] = row[6]
            dataLocation["driver"] = row[7]
            dataLocation["access_key"] = row[8]
            dataLocation["secret_key"] = row[9]
            dataLocation["port"] = row[10]
            dataLocation["bucket"] = row[11]
            dataLocation["db"] = row[12]

        cursor.close()
        return dataLocation

    def getStore(self, store):

        cursor = self.connection.cursor()

        query = "select name,type,schemaname,actual_name,\"table\",container from datagov.data_store where name='" + store + "'"
        cursor.execute(query)
        results = cursor.fetchall()

        dataStore = {}
        for row in results:
            dataStore["name"] = row[0]
            dataStore["type"] = row[1]
            dataStore["schemaname"] = row[2]
            dataStore["actual_name"] = row[3]
            dataStore["table"] = row[4]
            dataStore["container"] = row[5]


        cursor.close()
        return dataStore

    def getDbData(self, dataLocation, dataStore):
        #conn = jaydebeapi.connect(dataLocation["driver"], dataLocation["url"],
         #                         "/libs", )
        cursor = self.connection.cursor()

        cursor.execute("select * from " + dataStore["name"])
        results = cursor.fetchall()

        data=[]
        for row in results:
            print(row)
            data.append(json.loads(row[1]))
        cursor.close()
        #conn.close()
        return data

    def getKafkaData(self, dataLocation, dataStore):
        return ''

    def getS3Data(self, dataLocation, dataStore):
        return ''

    def getHdfsData(self, dataLocation, dataStore):
        return ''

    def getSolrData(self, dataLocation, dataStore):
        return ''

    def getEsData(self, dataLocation, dataStore):
        es = Elasticsearch()
        res = es.search(index="test-index", body={"query": {"match_all": {}}})
        print("Got %d Hits:" % res['hits']['total'])
        for hit in res['hits']['hits']:
            print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])
        return ''

    def getHbaseData(self, dataLocation, dataStore):
        return ''

    def getData(self, dataLocation, dataStore):

        if dataLocation["type"] == 'TABLE':
            return self.getDbData(dataLocation, dataStore)
        elif dataLocation["type"] == 'es':
            return self.getEsData(dataLocation, dataStore)
        else:
            print("Error")
            return [];

    '''
    {
    	"type": "inline",
    	"data": {
    		"columns": ["name", "rawText", "tags"],
    		"datatypes": ["string", "string", "string"],
    		"labelColumn": "tags",
    		"data": [
    			["abc", "ssss", "0"],
    			["kkk", "ddd", "1"]
    		]
    	}
    }

    {
    	"type": "source",
    	"data": {
    		"location": "mytest,
    		"store": "docs",
    		"dataFormat": "Json",
    		"labelColumn": "tags"
    	}
    }
    '''

    def fetchData(self, req):
        if(req["type"]=='inline'):
            return req["data"]
        else:
            return self.fetch(req["location"],req["store"])

    def fetch(self, location, store):
        # first fetch location & store objects from metadata

        # fetch data in jsontable
        dataLocation = self.getLocation(location)
        dataStore = self.getStore(store)

        return self.getData(dataLocation, dataStore)

    def loadDbData(self):
        data = {
            "type": "source",
            "data": {
                "location": "mytest",
                "store": "docs",
                "dataFormat": "Json",
                "labelColumn": "tags"
            }
        }
        ''' if(data.type!='inline'):
            data.type='inline'
            data.records=fetch(data.data.location,data.data.store)'''

        cursor = self.connection.cursor()
        query = "select * from docs"
        cursor.execute("desc mytest.docs")
        columns = cursor.fetchall()

        cols = []
        for column in columns:
            print(column)
            cols.append(column[0])

        numberOfColumns = len(cursor.fetchall())

        cursor.execute(query)

        results = cursor.fetchall()
        print(results)

        resultData = []
        for row in results:
            rowdata = []
            index = 0
            for column in columns:
                print(row[index])
                rowdata.append(row[index])
                index += 1
            resultData.append(rowdata)
        cursor.close()

        newData = {
            "columns": cols,
            "data": resultData
        }
        print(newData)

    def insert(self):
        cursor = self.connection.cursor()
        cursor.executemany(
            """INSERT INTO breakfast (name, spam, eggs, sausage, price)
            VALUES (%s, %s, %s, %s, %s)""",
            [
                ("Spam and Sausage Lover's Plate", 5, 1, 8, 7.95),
                ("Not So Much Spam Plate", 3, 2, 0, 3.95),
                ("Don't Wany ANY SPAM! Plate", 0, 4, 3, 5.95)
            ])

    def uploadModelPackage(self, model_id, path, filename,status, reason, numRecords):
        zipdir(path,filename)
        # read file
        data = read_file(filename)

        # prepare update query and data
        query = "UPDATE datagov.ml_model_instance " \
                "SET package_file = %s, status = %s, reason = %s, num_records = %s" \
                "WHERE id  = %s"

        args = (data, model_id,status, reason, numRecords)

        try:
            cursor = self.connection.cursor()
            cursor.execute(query, args)
            self.connection.commit()
        except Error as e:
            print(e)
        finally:
            cursor.close()
            os.remove(filename)

    def downloadModelPackage(self, model_id, path, filename):
        cursor = self.connection.cursor()
        query = "select package_file from datagov.ml_model_instance where id = %s"
        args = (model_id,)
        cursor.execute(query,args)
        package_file = cursor.fetchone()[0]
        # write blob data into a file
        write_file(package_file, filename)
        z = zipfile.ZipFile(filename)
        z.extractall()
        os.remove(filename)
        cursor.close()

    def updateStatus(self, model_id, status, reason, numRecords):
        # prepare update query and data
        query = "UPDATE datagov.ml_model_instance " \
                "SET status = %s, reason = %s, num_records = %s " \
                "WHERE id  = %s"

        args = (status, reason, model_id, numRecords)

        try:
            cursor = self.connection.cursor()
            cursor.execute(query, args)
            self.connection.commit()
        except Error as e:
            print(e)
        finally:
            cursor.close()

    def updateValidationStatus(self, model_id, status, reason, report, numRecords):
        # prepare update query and data
        query = "UPDATE datagov.ml_model_validation_instance " \
                "SET status = %s, reason = %s, report = %s , num_records = %s " \
                "WHERE id  = %s"
        print(query)
        args = (status, reason, report, model_id, numRecords)

        try:
            cursor = self.connection.cursor()
            cursor.execute(query, args)
            self.connection.commit()
        except Error as e:
            print(e)
        finally:
            cursor.close()

    def loadModelInstance(self, id):
        data = {
            "type": "source",
            "data": {
                "location": "mytest",
                "store": "docs",
                "dataFormat": "Json",
                "labelColumn": "tags"
            }
        }
        ''' if(data.type!='inline'):
            data.type='inline'
            data.records=fetch(data.data.location,data.data.store)'''

        cursor = self.connection.cursor()
        query = "select params, traindata from datagov.ml_model_instance where id="+id
        cursor.execute(query)

        results = cursor.fetchall()
        print(results)

        modelInstance={}

        for row in results:
            modelInstance["params"]=row[0]
            modelInstance["traindata"]=row[1]
            break
        cursor.close()

        return modelInstance

