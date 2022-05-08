import requests
import pandas as pd
import time
import json
from functions.utils import convertToBinaryData, encriptLanguage


def getLanguagesCountries():
    # Get data from link
    try:
        return requests.get('https://restcountries.com/v2/all?fields=region,name,languages').json()
    except Exception as e:
        return e

def createObjToTable(data):
    # Create an empty array
    table = []
    # for country in the response, search the language or the languages 
    for country in data:
        for lang in country['languages']:
            # start count time to get data and encript the language in ns
            start = time.perf_counter_ns()
            objToTable = {}
            objToTable['region'] = country['region']
            objToTable['cityName'] = country['name']
            objToTable['language'] = encriptLanguage(lang['name'])
            # stop count time to get data and encript the language in ns
            end = time.perf_counter_ns()

            # divide de ns in 1e+6 to get ms
            objToTable['time'] = ((end - start) / 1e+6)
            table.append(objToTable)
    return table


def createDataFrame(dataSet, path):
    # Create the Df from the dataSet in the parameters

    df = pd.DataFrame(dataSet)
    # Get the stadistic data
    stadistics = df.agg(
        {
            "time": ["min", "max", "mean"],
        })
    # Get the total time 
    statTotal = df.sum(axis = 0, skipna = True) 
    # Create a object to get data to insert into DB
    obj = {
        "minimo": stadistics.values[0][0],
        "maximo": stadistics.values[1][0],
        "promedio": stadistics.values[2][0],
        "total" : statTotal.values[3]
    }
    # Save the files
    # Save in Excel
    df.to_excel(path + "\DATA\dataToFrame.xlsx")
    # Change the extension and the name of the file to save
    path2 = path + "\DATA\dataToFrame.json"
    path = path + "\DATA\data.json"
    # Save Df to Json with pandas
    df.to_json(path2)

    # Save data in json
    with open(path, 'w') as outfile:
        json.dump(dataSet, outfile)
        # Get the json from the save faile
        obj['dataOnJson'] = convertToBinaryData(path)
    # Get the dfJson from the save faile
    obj['dataOnDfToJson'] = convertToBinaryData(path2)

    return obj


def createDatabase(MyDatabase):
    # Create the DB
    dbms = MyDatabase.MyDatabase(
        MyDatabase.SQLITE, dbname='pruebatecnica.sqlite')

    # Call the function to create Tables
    dbms.create_db_tables()

    return dbms

def insertResults(objToInsertIntoDB, gestorDB):
    # Create the query
    sqlite_insert_blob_query = """ INSERT INTO countries (minTime, maxTime, promTime, totalTime, dataOnJson, dataDfToJson) VALUES (?, ?, ?, ?, ?, ?)"""

    # Convert data into tuple format
    data_tuple = (objToInsertIntoDB['minimo'],
                  objToInsertIntoDB['maximo'], objToInsertIntoDB['promedio'], objToInsertIntoDB['total'], objToInsertIntoDB['dataOnJson'], objToInsertIntoDB['dataOnDfToJson'])
    
    # call the function to insert the object
    gestorDB.execute_query(data_tuple, sqlite_insert_blob_query)
