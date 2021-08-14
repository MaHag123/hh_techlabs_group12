#!/usr/bin/env python
# coding: utf-8

# In[105]:


#1. Get a dataset of all Zählstellen
import json
import pandas as pd
import requests
import numpy as np


# In[106]:


#read the data from the web
response = requests.get("https://iot.hamburg.de/v1.1/Datastreams?$filter=properties/serviceName eq 'HH_STA_AutomatisierteVerkehrsmengenerfassung' and properties/layerName eq 'Anzahl_Kfz_Zaehlstelle_1-Tag'&$top=500")


# In[107]:


#Turn the JSON response in usable data
#Extract the data from the the response object
betterdata=response.json()
#only select the data, which are under the heading "value" and convert this to a pandas dataframe
test = betterdata["value"]
df = pd.DataFrame.from_dict(test)
df.head()


# In[108]:


#2. Loop through the links of all "Zählstellen" to receive a dataset of the nr. of vehicles for every day/every "Zählstelle"

## generate links and append to df
df["links"] = df["Observations@iot.navigationLink"].apply(lambda x: x + "?$top=600")
df['id'] = df['@iot.id']

# clean dataframe where we dont find geo information
# 2 rows should be removed
print(f'Rows before NA removal: {len(df)}')
df = df.dropna()
print(f'Rows after NA removal: {len(df)}')
# extract the lat/long information to join later
df['long'] = df['observedArea'].apply(lambda x: x['coordinates'][0])
df['lat'] = df['observedArea'].apply(lambda x: x['coordinates'][1])


# In[109]:


# create a list for the results
columns = ['@iot.id','phenomenonTime', 'result','resultTime']
missing_data = list()
df_complete = pd.DataFrame(columns = columns)

unique_links = df["links"].unique()

for link in unique_links:
    print(f'Retrieving data for {link}')
    response = requests.get(link)
    json = response.json()
    df_temp = pd.DataFrame.from_dict(json["value"])
    if set(columns).issubset(set(df_temp.columns)):
        df_temp = df_temp[columns]
        df_temp['id'] = link[40:45]
        df_complete = df_complete.append(df_temp,sort=False)
#        df_complete['id'] = df_complete['id'].astype('int64')
    else:
        # no data available
        print(f'---- WARNING: No data available or wrong columns selected for {link}')
        missing_data.append(link)
df_complete['id'] = df_complete['id'].astype('int64')
df_complete['result'] = df_complete['result'].astype('int64')


# In[110]:


missing_data


# In[111]:


len(df_complete)


# In[112]:


df_complete.head()


# In[113]:


df_raw = pd.merge(df_complete,df[['id','lat','long']], how='inner',on='id')
                        


# In[114]:


df_raw.head()


# In[115]:


df_raw.loc[df_raw['id']!=13600]


# ## Debug section

# In[116]:


# only for debugging
columns_test = ['@iot.id','phenomenonTime', 'result','resultTime']
missing_data_test = list()
df_complete_test = pd.DataFrame(columns = columns_test)

# first link doesnt return data, second one works
unique_links_test = ['https://iot.hamburg.de/v1.1/Datastreams(13196)/Observations?$top=600',
                     'https://iot.hamburg.de/v1.1/Datastreams(13596)/Observations?$top=600']

for link in unique_links_test:
    print(f'Retrieving data for {link}')
    response = requests.get(link)
    json = response.json()
    df_temp_test = pd.DataFrame.from_dict(json["value"])
    if set(columns_test).issubset(set(df_temp_test.columns)):
        df_temp_test = df_temp_test[columns_test]
        df_temp_test['id'] = link[40:45]
        df_complete_test = df_complete_test.append(df_temp_test,sort=False)
        df_complete_test['id'] = df_temp_test['id'].astype('int64')
    else:
        # no data available
        print(f'---- WARNING: No data available or wrong columns selected for {link}')
        missing_data_test.append(link)


# In[117]:


df_raw_test = pd.merge(df_complete_test,df[['id','lat','long']], how='inner',on='id')
                        


# In[118]:


df_raw_test


# Visualisierung - Patricia
# 1. Wochentage rausfinden und Durchschnitt über Wochentage legen

# In[119]:


weekday_list=[]

for i in df_raw['phenomenonTime']:
    t = i[25:35]
    day = pd.Timestamp(t)
    weekday = day.day_name()
    print(weekday)

weekday_list.append(weekday)


# In[ ]:





# Meistens kann man die apply Funktion für solche Fälle nutzen, also wenn man für jeden Wert oder jede Reihe etwas anwenden will.
# Konkret im unteren Beispiel wird `apply(lambda x: get_weekday(x))` für jeden Wert in `df_raw['phenomenonTime']` angewendet und `return weekday`. Das kann dann einfach in eine neue Spalte gespeichert werden (siehe df['weekday'], dieser Key muss vorher nicht existieren)

# In[120]:


# Dein Code nur als Funktion
def get_weekday(input_date):
    t = input_date[25:35]
    day = pd.Timestamp(t)
    weekday = day.day_name()
    return weekday


# In[121]:


# lass das laufen und du siehst was der Output ist
# ich nutze df_raw_test, damit das etwas schneller ist
df_raw_test['phenomenonTime'].apply(lambda x: get_weekday(x))


# ## Debug Ende

# In[ ]:





# In[122]:


# neue Spalte im Dataframe
df_raw['weekday'] = df_raw['phenomenonTime'].apply(lambda x: get_weekday(x))


# In[123]:


df_raw.head()


# Maren
# Goal: Create "clean" csv file, so everyone can work with that from now on
# 
# advantages: 
# - no useless columns anymore
# - the downloaded data/dates don´t change any more
# - otherwise with every time running the codes a dataset with daily updated data occurs, which might confuse our results
# 

# In[124]:


# convert the dates to a correct format
phenomenonTime = df_raw["phenomenonTime"]

phenomenonTime=phenomenonTime.astype(str)
phenomenonDate=phenomenonTime.str.replace("(.*)\/", "", regex = True)
phenomenonDate=phenomenonDate.str.replace("T(.*)", "", regex = True)

df_raw["phenomenonDate"] = pd.to_datetime(phenomenonDate)


# In[125]:


df_raw.dtypes


# In[126]:


# Keep only relevant columns
columns = ['@iot.id','phenomenonDate', 'result','id','lat','long','weekday']


# In[127]:


df_raw = df_raw[columns]


# In[128]:


df_raw


# Maren: Clean rest of the dataset
# - Are there results with 0 ? --> Drop
# - Are there duplicates in Zählstellen/ phenomenondate? --> Drop one of them

# In[129]:


#drop all observations which have 0 as result
df_raw[df_raw.result!=0]


# In[130]:


#find problematic observations
#Are there duplicates in combination Zählstellen/ phenomenondate? 


## pivot: count numbers of Zählstellen("id") per phenomenondate --> saved as CountZählstelle
CountZaehlstelle = df_raw.pivot_table(values = "id", index = "phenomenonDate", aggfunc= "count")

import seaborn as sns
import matplotlib.pyplot as plt

sns.lineplot(data=CountZaehlstelle, x = "phenomenonDate", y = "id")

CountZaehlstelle
## find the problematic dates: There were measured almost double the Zählstellen than on the other dates
CountZaehlstelle[CountZaehlstelle.id > 500]


# In[131]:


##--> problematic dates are: 2021-03-27 and 2021-07-01
Problemdata_a = df_raw[df_raw.phenomenonDate == "2021-03-27"]
Problemdata_b = df_raw[df_raw.phenomenonDate == "2021-07-01"]

## pivot: Find out whether the 2 entries for every Zählstelle on 2021-03-27 and 2021-07-01 are so close together that we can just drop one of each
Problemdata_apiv = Problemdata_a.pivot_table(values = "result", index = "id", aggfunc = lambda x: max(x) - min(x))
Problemdata_apiv[Problemdata_apiv.result > 100]


# In[133]:


Problemdata_bpiv = Problemdata_b.pivot_table(values = "result", index = "id", aggfunc = lambda x: max(x) - min(x))
Problemdata_bpiv[Problemdata_bpiv.result > 100]

##--> result: yes, we can drop duplicates in the dataset according to id/phenomenonDate 
## because the difference in result between the duplicates is not so big


# In[137]:


#drop duplicates
df_raw=df_raw.drop_duplicates(['phenomenonDate','id'],keep= 'first')


# In[138]:


## pivot: count numbers of Zählstellen("id") per phenomenondate 
CountZaehlstelle_final = df_raw.pivot_table(values = "id", index = "phenomenonDate", aggfunc= "count")


sns.lineplot(data=CountZaehlstelle_final, x = "phenomenonDate", y = "id")


# In[139]:


#save in csv format
df_raw.to_csv(r'C:\Users\Maren\Desktop\TechLabs\hh_techlabs_group12\hh_techlabs_group12\clean_data_2021-08-14.csv',index=False)


# In[ ]:




