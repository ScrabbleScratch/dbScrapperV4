#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#----------------------------------------------------------------------------
# Title: dbScrapperV4
# Author: ScrabbleScratch
# Url: https://github.com/ScrabbleScratch/dbScrapperV4
# Created: 09/november/2021
# version: 4
#License: GNU General Public License v3.0
# ---------------------------------------------------------------------------
""" The module handles requests to an API and parses the data to be able to
insert it into a MySQL database server. The script is able to query data from
the database based on a _uniqueId_ specified in its configuration file,
comparing if the data is the same. It can update or create new entries within
the database depending on the comparisson result between the data. """
# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
#!/usr/bin/env python3
import json
from mysql.connector.utils import NUMERIC_TYPES
import requests as rq
import mysql.connector
from time import sleep

# default database config
defDataBaseConfig = {"api":"","database":{"host":"","port":3306,"user":"","password":"","database":""},"table":"","dbUnique":"","columns":{"API":"DB"},"delay":2}

# create the dbScrapper class
class dbScrapper():
    # initialize class instance
    def __init__(self, config, delay=False):
        # load configuration parameters from json file
        try:
            with open(config, 'r') as file:
                conf = json.loads(file.read())
        except:
            print("Database config not found!")
            with open(config, "w") as file:
                file.write(json.dumps(defDataBaseConfig, indent=4))
            exit()
        # initialize database connection and create a MySQLConnection object
        self.db = mysql.connector.connect(
            host = conf["database"]["host"],
            port = conf["database"]["port"],
            user = conf["database"]["user"],
            password = conf["database"]["password"],
            database = conf["database"]["database"]
        )
        # self.db cursor
        self.dbCursor = self.db.cursor()
        # api url
        self.api = conf["api"]
        # database table name
        self.dbTable = conf["table"]
        # database unique column
        self.uniqueId = conf["dbUnique"]
        # (api) : (database) dictionary
        self.apiCols = conf["columns"]
        # keys only from self.apiCols
        self.apiKeys = list(conf["columns"].keys())
        # cycle delay
        if delay:
            print(f"* dbScrapper initialized with custom delay of {delay}")
            self.delay = delay
        else:
            self.delay = conf["delay"]
        # (database) : (api) dictionary
        self.dbCols = {}
        for k in self.apiKeys:
            self.dbCols[self.apiCols[k]] = k
        # keys only from self.dbCols
        self.dbKeys = list(self.dbCols.keys())
        # columns in database table
        self.dbCursor.execute(f"DESCRIBE `{conf['table']}`")
        cols = list(self.dbCursor.fetchall())
        self.tableCols = []
        for c in cols:
            self.tableCols.append(c[0])
        print("New dbScrapper object created!")
        return
    
    # fetch data info from self.api
    # returns 'data' (json formatted data) if it found a result, and False if it did not found anything
    def dataGet(self, fetch):
        data = rq.get(f"{self.api}/{fetch}")
        sleep(self.delay)
        if data.status_code in [200, 201]:
            print(f"Entry with Id:{fetch} found!")
            data = data.json()
            return data
        elif data.status_code == 404:
            print(f"Entry with Id:{fetch} not found!")
        elif data.status_code in [400, 401, 403, 405, 409]:
            print(f"Invalid request!")
        elif data.status_code in [500, 503]:
            print(f"Service not available right now!")
            return None
        else:
            print("Unknown status code: " + data.status_code)
        return False
    
    # check if data (dictionary parsed from API request) is already in the database table (self.dbTable) and if data values are the same (shows each column)
    # returns result (data exists on database or not), different (data in database is different than the one being checked), query(returns the database query)
    def dataExists(self, data):
        print("Checking data existance within the database...")
        self.dbCursor.execute(f"SELECT * FROM `{self.dbTable}` WHERE `{self.uniqueId}`={data[self.dbCols[self.uniqueId]]}")
        query = self.dbCursor.fetchall()
        # return False if there is no result
        if not query: return False
        # database query column dictionary
        queryDict = {}
        for k in range(len(self.tableCols)):
            queryDict[self.tableCols[k]] = query[0][k]
        # entire content of the query as list of dcitionaries pero row in database
        queryContent = []
        for r in query:
            row = {}
            for c in range(len(self.tableCols)):
                row[self.tableCols[c]] = r[c]
            queryContent.append(row)
        #print(query)
        result = False
        different = False
        # compares data from database and parsed
        print(f"\t├─Same '{self.uniqueId}' found in database!")
        dataKeys = list(data.keys())
        for k in self.tableCols:
            queryVal = queryDict[k]
            if k in dataKeys:
                if data[self.dbCols[k]] == "null": data[self.dbCols[k]] = None
                if (type(queryVal) not in NUMERIC_TYPES and str(queryVal) == str(data[self.dbCols[k]]).replace("\\","")) or \
                    (type(queryVal) in NUMERIC_TYPES and float(queryVal) == float(data[self.dbCols[k]])):
                    colCheck = True
                    result = True
                else:
                    colCheck = False
                    different = True
                print(f"\t│\t├─Same {k}: {colCheck}")
        print(f"\t└─Data exists in database:\n\t\t├─result:{result}\n\t\t└─different:{different}")
        return [result,different,queryContent]
    
    # insert or update an anime entry in the database and finally check if it is found int he database
    # prints id, mal_id and title if the entry was added and found in the database
    def dataInsert(self, data):
        if not type(data) is dict:
            print("\t└─Invalid data to insert! Returning...")
            return False
        dataKeys = list(data.keys())
        for c in self.tableCols:
            k = self.dbCols[c]
            if k in dataKeys:
                if data[k] == None: data[k] = "null"
                if type(data[k]) is bool: data[k] = int(data[k])
                if not str(data[k]).isnumeric():
                    print(f"\t├─To be changed: {k}")
                    data[k] = str(data[k]).replace('"', '\\"')
        check = self.dataExists(data)
        if not check:
            print("Entry not found in the database! Creating it...")
            dbEntry = f"INSERT INTO `{self.dbTable}` VALUES ("
            for c in self.tableCols:
                k = self.dbCols[c]
                if k in dataKeys:
                    if not str(data[k]).isalpha() and str(data[k]).isnumeric() or data[k] == "null":
                        dbEntry += f"{data[k]}"
                    else:
                        dbEntry += f"\"{data[k]}\""
                else:
                    dbEntry += "null"
                if c != self.apiKeys[-1]: dbEntry += ","
            dbEntry += ")"
            #print(dbEntry)
            self.dbCursor.execute(dbEntry)
            print("\t├─Checking data added succesfully... ")
        elif check[0] and check[1]:
            print("Entry was found with different values! Updating it...")
            dbUpdate = f"UPDATE `{self.dbTable}` SET "
            for c in self.tableCols:
                k = self.dbCols[c]
                if k in dataKeys:
                    if str(data[k]).isnumeric() or data[k] == "null":
                        dbUpdate += f"`{c}`={data[k]}"
                    else:
                        dbUpdate += f"`{c}`=\"{data[k]}\""
                    if c != self.apiKeys[-1]: dbUpdate += ", "
            dbUpdate += f" WHERE {self.uniqueId}={data[self.dbCols[self.uniqueId]]}"
            #print(dbUpdate)
            self.dbCursor.execute(dbUpdate)
            print("\t├─Checking data was updated succesfully... ")
        else:
            print("Entry was found in the database with no changes!")
            return False
        self.db.commit()
        entry = self.dataExists(data)
        if not entry:
            print("Entry was not found in the database! Check the system.")
            return False
        elif entry[0] and not entry[1]:
            if len(entry[2]) == 1:
                print(f"Entry was found in the database!\n\t└─{self.uniqueId}: {entry[2][0][self.uniqueId]}\n")
                return True
            elif len(entry[2]) > 1:
                print("Entry was found multiple times in the database! Please check database.")
        elif entry[0] and entry[1]:
            print("Entry was found with different values! Check the system.")
        return None
    
    # close the connection with the database
    def closeConnection(self):
        self.db.close()
        return

# run the next set of commands to execute it properly
#scrapper = dbScrapper(configFile)
#data = scrapper.dataGet(1)
#scrapper.dataInsert(data)
#scrapper.closeConnection()
