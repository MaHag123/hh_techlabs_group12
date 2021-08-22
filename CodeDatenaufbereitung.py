#1. Get a dataset of all Zählstellen

#%%
import json
import pandas as pd
import requests
import numpy as np
import re

#%%
#read the data from the web
response = requests.get("https://iot.hamburg.de/v1.1/Datastreams?$filter=properties/serviceName eq 'HH_STA_AutomatisierteVerkehrsmengenerfassung' and properties/layerName eq 'Anzahl_Kfz_Zaehlstelle_1-Tag'&$top=500")
  #--> API Status Code 200: Everything went okay
print(response.json())
print("hello")
#%%
#Turn the JSON response in usable data
#Extract the data from the the response object
betterdata=response.json()
  #type(betterdata)
  #--> type: dict

print(betterdata)
#%%
#only select the data, which are under the heading "value" and convert this to a pandas dataframe
test = betterdata["value"]
  #type(test)
  #--> type: List
 
#%%
firstdataframe = pd.DataFrame(test)
firstdataframe.columns

firstdataframe

#%%
#save Zählstellen IDs in a panda series for easier use later
iotids = firstdataframe["@iot.id"]

#save Zählstellen Download-Links in a panda series for easier use later
Links = firstdataframe["Observations@iot.navigationLink"]

#%%
#Extract coordinates in a correct format
ObservedArea = firstdataframe["observedArea"].astype(str)

#%%
Observed1 = ObservedArea.str.replace("(.*)\: \[", "", regex = True)

Observed2 = Observed1.str.replace("\]\}", "", regex = True)

Longitude = Observed2.str.replace("\,(.*)", "", regex = True).astype(float)

Latitude = Observed2.str.replace("(.*)\, ", "", regex = True).astype(float)


#%%

#2. Loop through the links of all "Zählstellen" to receive a dataset of the nr. of vehicles for every day/every "Zählstelle"

#first every Zählstelle is assigned a number (Sequence) to use in the loop (e.g. 493 Zählstellen --> sequence of 0 to 492)
NoZaehlstellen = len(Links.index)

Sequence = np.array(range(0,NoZaehlstellen))
# create a list for the results
ListofAll = list()

#%%
for x in Sequence:
  #by default only the first 100 observations for this "Zählstelle" will be shown, therefore we expand the results to max 600
  Testlink = Links[x]+"?$top=600"
  
  #read the data from the web
  Fall = requests.get(Testlink)
    #--> API Status Code 200: Everything went okay
  #Extract the data from the the Fall-object
  FallData = Fall.json()
  #only select the data, which are under the heading "value" and convert this to a pandas dataframe
  FallDataValue = FallData["value"]
  FallDataFrame = pd.DataFrame(FallDataValue)
  #New column is created with the IDs for each Zählstelle
  FallDataFrame["ZaehlstelleID"] = iotids[x]
  FallDataFrame["Longitude"] = Longitude[x]
  FallDataFrame["Latitude"] = Latitude[x]
  #all observations from all Zählstellen are appended to the final list
  ListofAll.append(FallDataFrame)
  

print(ListofAll)
#%%
#Convert list to one single dataframe 
DataofAll = pd.concat(ListofAll)

DataofAll
#%%

# convert the dates to a correct format
phenomenonTime = DataofAll["phenomenonTime"]

phenomenonTime=phenomenonTime.astype(str)
phenomenonDate=phenomenonTime.str.replace("(.*)\/", "", regex = True)
phenomenonDate=phenomenonDate.str.replace("T(.*)", "", regex = True)

DataofAll["phenomenonDate"] = pd.to_datetime(phenomenonDate)


# Drop unnecessary columns
DataofAll.drop(columns=['phenomenonTime', 'resultTime','@iot.selfLink','Datastream@iot.navigationLink', 'MultiDatastream@iot.navigationLink','FeatureOfInterest@iot.navigationLink'], inplace=True)


# Only the following columns are in the dataset now: '@iot.id' (every single observation has an individual ID), 'result' (number of vehicles),
# 'ZaehlstelleID' (every ZählstelleID has multiple iot.ids), 'Longitude', 'Latitude','phenomenonDate' (date on which the vehicles are counted)

# next steps: Descriptive statistics, deal with missing data, deal with outliers?, .... , afterwards visualization
   
