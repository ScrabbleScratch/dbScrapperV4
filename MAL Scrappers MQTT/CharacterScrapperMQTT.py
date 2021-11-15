#!/usr/bin/env python3
from dbScrapperV4 import dbScrapper
from paho.mqtt import publish
from datetime import datetime
from os import mkdir
import json
import argparse
import logging

# create config folder
try:
    mkdir("config/")
except:
    pass

# create logs folder
try:
    mkdir("logs/")
except:
    pass

# create mqtt url file
try:
    with open("config/mqtt.txt", "x"):
        print("mqtt.txt created")
    with open("config/mqtt.txt", "w") as f:
        f.write("mqtt.eclipseprojects.io|/ssserver/characterscrapper/status")
except:pass

# read mqtt url file
with open("config/mqtt.txt","r") as f:
    data = f.read().split("|")
    brokerUrl = data[0]
    statusTopic = data[1]
    print(f"MQTT:\n\tURL: {brokerUrl}\n\tTopic: {statusTopic}")

# configure logging
logging.basicConfig(filename="logs/CharacterScrapper.log", \
    format="%(asctime)s (%(levelname)s): %(message)s", \
        datefmt='%d/%m/%Y (%a) %H:%M:%S >> ', level=logging.DEBUG)

# define console parameters to be parsed
parser = argparse.ArgumentParser()
parser.add_argument("-s", "--start", help="Specify an Id to start from", type=int)
parser.add_argument("-c", "--cycle", help="Specify a cycle delay", type=float)
args = parser.parse_args()

# files paths
statusFile = "config/status.json"
dbConfigFile = "config/scrapper-conf-V4-character.json"

# if parameters parsed then define variables, if not read status file
if args.start:
    finished = False
    lastId = args.start
    logging.debug(f"Got lastId = {lastId} from parsed argument!")
    print(f"-> Got lastId = {lastId} from parsed argument!")
else:
    # load status file to check for previus program status
    while True:
        try:
            with open(statusFile, "r") as file:
                logging.debug("Status file opened")
                print("Status file opened!")
                status = file.read()
                # if file content is not spaces and its length is grater than 0 then read content
                if not status.isspace() and len(status) > 0:
                    status = json.loads(status)
                    statusKeys = status.keys()
                    # if the needed keys are within the content then continue
                    if "finished" in statusKeys and "lastId" in statusKeys and "maxId" in statusKeys:
                        # if the finished parameter is False then continue
                        if not status["finished"]:
                            finished = status["finished"]
                            lastId = status["lastId"]
                            maxId = status["maxId"]
                            logging.info(f"Finished: {finished}, LastId: {lastId}, Max Id: {maxId}")
                            #print(finished, lastId)
                            break
                # if something fails raise an error
                raise FileNotFoundError
        # if the FileNotFound is raised then create a fresh status file with default parameters
        except FileNotFoundError:
            logging.debug("Status file not found or unusable")
            print("Status file not found or unusable!")
            with open(statusFile, "w") as file:
                logging.debug("Creating status file")
                print("Creating status file...")
                file.write(json.dumps({"finished":False, "lastId":0, "maxId":50000}, indent=4))

# create a dbScrapper object
logging.debug("Creating dbScrapper object")
characterScrapper = dbScrapper(dbConfigFile, args.cycle)

# function to update mqtt status
def mqttUpdate(message):
    try: publish.single(statusTopic, str(datetime.now())+">>"+str(message), 2, True, brokerUrl)
    except: print("Couldn't update mqtt status!")
    return

# if finished is false then continue
if not finished:
    logging.debug(f"Scrapping character data from Id: {lastId} to Id: {maxId}")
    print(f"Scrapping character data from Id: {lastId} to Id: {maxId}")
    try:
        # scrap data from lastId to maxId
        for x in range(lastId, maxId):
            # bucle until characterData gets valid data to evaluate
            while True:
                logging.debug(f"Getting Id: {x} data")
                characterData = characterScrapper.dataGet(x)
                # if characterData has valid data, insert it into the database and update the status file
                if characterData:
                    logging.debug("Valid data")
                    insertStatus = characterScrapper.dataInsert(characterData)
                    if insertStatus:
                        logging.debug("Data inserted succesfully into database")
                        mqttUpdate(f"Id: {x} data inserted succesfully into database")
                    else:
                        logging.error("Error while inserting data into database!")
                        mqttUpdate(f"Error while inserting Id: {x} data into database!")
                    break
                elif characterData is False:
                    logging.debug("Invalid data")
                    break
            # update status
            with open(statusFile, "w") as status:
                logging.debug("Updating status file")
                status.write(json.dumps({"finished":False, "lastId":x, "maxId":maxId}, indent=4))
            mqttUpdate("Last Id: "+str(x))
    except Exception as e:
        error = "An error occurred while running the program!\nError: "+str(e)+"\nTerminating program..."
        logging.error(error)
        print(error)
        mqttUpdate(error)
        exit()
    # when the maxId has been reached, update the finished parameter to True within the status file
    with open(statusFile, "w") as status:
        logging.debug("Update status file to finished")
        status.write(json.dumps({"finished":True, "lastId":x, "maxId":maxId}, indent=4))

# close the database connection when everything has finished
logging.debug("Closing database connection")
characterScrapper.closeConnection()

# print finished message
logging.info("Scrapping is finished!")
print("Scrapping is finished!")
mqttUpdate("Scrapping is finished!")
