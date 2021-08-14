#!/usr/bin/env python
# coding: utf-8

# # Descriptives (using the csv file)

# In[163]:


import json
import pandas as pd
import requests
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


# # Maren´s part

# In[164]:


#get the csv


# In[165]:


df_clean_data = pd.read_csv(r'C:\Users\Maren\Desktop\TechLabs\hh_techlabs_group12\hh_techlabs_group12\clean_data_2021-08-14.csv')


# In[166]:


df_clean_data


# In[167]:


df_clean_data.info()


# In[168]:


df_clean_data.dtypes


# In[169]:


#  I would like to change phenomenonDate back to the format datetime64 (this got lost when saving the file as csv)


# In[170]:


df_clean_data.astype({'phenomenonDate': 'datetime64'}).dtypes


# In[171]:


#min/max values

df_clean_data.max()


# In[172]:


df_clean_data.min()


# In[173]:


### --> range of dates: From 2020-11-01 to 2021-08-13 the vehicles are counted


# In[174]:


# How many Zählstellen are measuring every day? 

## pivot: count numbers of Zählstellen("id") per phenomenondate
CountZaehlstelle = df_clean_data.pivot_table(values = "id", index = "phenomenonDate", aggfunc= "count")
CountZaehlstelle


# In[175]:


sns.lineplot(data=CountZaehlstelle, x = "phenomenonDate", y = "id")


# In[176]:


# Combined number of cars measured each individual day
SumCars = df_clean_data.pivot_table(values = "result", index = "phenomenonDate", aggfunc = "sum")
SumCars


# In[177]:


sns.barplot(x="phenomenonDate", y="result", data=SumCars)

### Doesn´t work! @Patricia, könntest du mal schauen, ob du das rausfindest? Ich hätte einfach gern das PhenomenonDate
# in der X-Achse und die Summen aus "SumCars" jeweils als Balken obendrüber :) LG, Maren


# In[178]:


# How much traffic is there all combined every day?
sns.catplot(x="phenomenonDate", y="result", data = df_clean_data, height=8, aspect=2.5)


# # Patricia´s part:

# In[179]:


# Amount of cars per weekday
sns.catplot(x="weekday", y="result", data = df_clean_data, height=8)


# In[180]:


# Another way for visualiszation is using a boxplot. 
sns.catplot(x="weekday", y="result", kind="box", data=df_clean_data, height = 8)


# In[181]:


# Calculating the average of each weekday
df_clean_data.groupby(['weekday']).mean()


# In[ ]:




