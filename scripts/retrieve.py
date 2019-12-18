# Enter password for MySQL RDS as first command line argument

from datetime import datetime

import sys
import requests
import urllib.request
import time
import json
import pymysql

# RDS
RDS_HOST = 'pm-mysqldb.cxjnrciilyjq.us-west-1.rds.amazonaws.com'
RDS_PORT = 3306
RDS_USER = 'admin'
RDS_DB = 'pricing'

# RDS PASSWORD
if len(sys.argv) == 2:
    RDS_PW = str(sys.argv[1])
else:
    print("Usage: Enter password for MySQL server as first command line argument")
    sys.exit(1)

print("Connecting to RDS...")

# RDS connection
conn = pymysql.connect( RDS_HOST,
                        user=RDS_USER,
                        port=RDS_PORT,
                        passwd=RDS_PW,
                        db=RDS_DB)
print("Connected to RDS successfully.")

cur = conn.cursor()
print("Initialized cursor.")

# JSON data file
json_data = 'jsondata.json'

# Initialize data structures for json data
responses = []
numberOfStores = 0                  # Also used as index for responses[]
#numberOfItemsPerStore = 20

# Hashmaps of access keys and store info
accessKeyDict = {'itemArrayAccessKeys': [], 'itemAccessKeys': [], 'priceAccessKeys': [], 'unitAccessKeys': [], 'isOnSaleAccessKeys': [], 'salePriceAccessKeys': []}
storeInfo = {'storeNames': [], 'storeZipCodes': []}

print("Initializing data structures...")

# Load JSON containing URLs of APIs of grocery stores
with open(json_data, 'r') as data_f:
    data_dict = json.load(data_f)

print("Opened JSON file successfully.")

# Organize API URLs
for apiurl in data_dict['apiURL']:
    responses.append('')
    responses[numberOfStores] = requests.get(apiurl['url'])
    responses[numberOfStores].raise_for_status()
    numberOfStores += 1
    storeInfo['storeNames'].append(apiurl['name'])
    storeInfo['storeZipCodes'].append(apiurl['zipCode'])
    accessKeyDict['itemArrayAccessKeys'].append(apiurl['itemArrayAccessKeys'])
    accessKeyDict['itemAccessKeys'].append(apiurl['itemAccessKeys'])
    accessKeyDict['priceAccessKeys'].append(apiurl['priceAccessKeys'])
    accessKeyDict['unitAccessKeys'].append(apiurl['unitAccessKeys'])
    if apiurl['saleAccessKeysExist']:
        accessKeyDict['isOnSaleAccessKeys'].append(apiurl['saleAccessKeysExist']['isOnSaleAccessKeys'])
        accessKeyDict['salePriceAccessKeys'].append(apiurl['saleAccessKeysExist']['salePriceAccessKeys'])
    else:
        accessKeyDict['isOnSaleAccessKeys'].append(None)
        accessKeyDict['salePriceAccessKeys'].append(None)

#print(accessKeyDict['isOnSaleAccessKeys'])
#print(accessKeyDict['salePriceAccessKeys'])

print("Retrieved data from API calls to grocery stores.")

# Get current date and time
def getCurrentDateAndTime():
    now = datetime.now()
    # YYYY/mm/dd HH:MM:SS
    return now.strftime("%Y/%m/%d %H:%M:%S")

# Get access keys for a JSON
def getKeys(data, keys, apiItemIndex):
    
    '''
    # Temporary fix for APIs missing data (i.e. Target units), fix ASAP
    if keys[0] == "N/A":
        return keys[1]
    '''
    
    # Known bug: cannot iterate NoneType (be careful with null keys)
    for key in keys:
        if key == "apiItemIndex":
            key = apiItemIndex
#       print(key)
        data = data[key]
    return data

# Number of items in array of items from store API
def getNumberOfItems(storeindex):
    numberOfItems = len(getKeys(responses[storeindex].json(), accessKeyDict['itemArrayAccessKeys'][storeindex], 0))
#   print(numberOfItems)
    return numberOfItems

# Return price of item after checking if item is on sale
def checkItemSale(accessKeyDict, storeindex, itemcount):
    if accessKeyDict['isOnSaleAccessKeys'][storeindex]:
        if getKeys(responses[storeindex].json(), accessKeyDict['isOnSaleAccessKeys'][storeindex], itemcount):
            return getKeys(responses[storeindex].json(), accessKeyDict['salePriceAccessKeys'][storeindex], itemcount)
    return getKeys(responses[storeindex].json(), accessKeyDict['priceAccessKeys'][storeindex], itemcount)

# Add items and prices
mysql_insert_groceries_query = """INSERT INTO groceries (item, price, unit, store, zipcode, price_date)
                        VALUES (%s, %s, %s, %s, %s, %s) """

print("Inserting to RDS...")

for storeindex in range(numberOfStores):
#   print("STORE")
#   print(storeindex)
#    for itemcount in range(numberOfItemsPerStore):
    for itemcount in range(getNumberOfItems(storeindex)):
        itemToAppend = getKeys(responses[storeindex].json(), accessKeyDict['itemAccessKeys'][storeindex], itemcount)
        unitsToAppend = getKeys(responses[storeindex].json(), accessKeyDict['unitAccessKeys'][storeindex], itemcount)
        priceToAppend = checkItemSale(accessKeyDict, storeindex, itemcount)
#        priceToAppend = getKeys(responses[storeindex].json(), accessKeyDict['priceAccessKeys'][storeindex], itemcount)
        storeToAppend = storeInfo['storeZipCodes'][storeindex]
        insertTuple = (itemToAppend, priceToAppend, unitsToAppend, storeInfo['storeNames'][storeindex], storeToAppend, getCurrentDateAndTime())
        cur.execute(mysql_insert_groceries_query, insertTuple)

conn.commit()
print("Committed insertion to RDS.")

cur.close()
print("Cursor closed.")

conn.close()
print("Connection to RDS closed.")

print("Script complete")
