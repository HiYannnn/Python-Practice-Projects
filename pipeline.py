import requests
import pandas as pd
import pprint
import sqlite3

# Extract Data

## requests whitewines
url="https://api.sampleapis.com/wines/whites"

try:
    res=requests.get(url)
    res.raise_for_status()
    whites=res.json()
    
except requests.exceptions.RequestException as e:
    print(f"Error fetching dtata:{e}")
    whites=[]
   
## requests redwines
url="https://api.sampleapis.com/wines/reds"
res=requests.get(url)
reds=res.json()

## requests sparklings
url="https://api.sampleapis.com/wines/sparkling"
res=requests.get(url)
sparkling=res.json()

## requests rosé
url="https://api.sampleapis.com/wines/rose"
res=requests.get(url)
rose=res.json()



# Transform Data

df_whites=pd.DataFrame(whites)
df_reds=pd.DataFrame(reds)
df_spark=pd.DataFrame(sparkling)
df_rose=pd.DataFrame(rose)

df_whites["wine_type"]="white"
df_reds["wine_type"]="red"
df_spark["wine_type"]="sparkling"
df_rose["wine_type"]="rose"

## concat 4 df together along rows,coz culumns r same
wines = pd.concat([df_whites, df_reds,df_spark,df_rose], axis=0)
wines.reset_index(drop=True, inplace=True)

## separate "location" into "country" and "location" columns
wines[["country","location"]] = wines["location"].str.split("\n·\n", n=1, expand=True)

## seperate average rating and number of reviews into 2 different columns from "rating" column,then drop original "rating"column
wines["avg_rating"]=wines["rating"].apply(lambda x:x["average"])
wines["reviews"]=wines["rating"].apply(lambda x:x["reviews"])
wines.drop("rating",axis=1,inplace=True)

## retriv number of reviews
wines["reviews"]=wines["reviews"].str.replace("ratings","").astype("int")
wines["reviews"]=pd.to_numeric(wines["reviews"])

## convert 'avg_rating' column to numeric type (float)
wines['avg_rating'] = pd.to_numeric(wines['avg_rating'], errors='coerce')



# Load Data into CSV and SQlite Database

wines.to_csv("wines_data.csv", index=False)

conn = sqlite3.connect("wines_data.db")

wines.to_sql("wines_data", conn, if_exists="replace", index=False)

conn.close()








