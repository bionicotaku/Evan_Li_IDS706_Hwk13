# Databricks notebook source
!pip install -r ../requirements.txt

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import requests
import json
import base64

display(dbutils.fs.ls('dbfs:/'))

# COMMAND ----------

load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")

# COMMAND ----------

headers = {'Authorization': 'Bearer %s' % access_token}
url = "https://"+server_h+"/api/2.0"
def perform_query(path, headers, data={}):
  session = requests.Session()
  resp = session.request('POST', url + path, data=json.dumps(data), verify=True, headers=headers)
  return resp.json()

def mkdirs(path, headers):
  _data = {}
  _data['path'] = path
  return perform_query('/dbfs/mkdirs', headers=headers, data=_data)

# does its own check to see if the file exists or not 
mkdirs(path="dbfs:/FileStore/nlp", headers=headers)

def create(path, overwrite, headers):
  _data = {}
  _data['path'] = path
  _data['overwrite'] = overwrite
  return perform_query('/dbfs/create', headers=headers, data=_data)

def add_block(handle, data, headers):
  _data = {}
  _data['handle'] = handle
  _data['data'] = data
  return perform_query('/dbfs/add-block', headers=headers, data=_data)

def close(handle, headers):
  _data = {}
  _data['handle'] = handle
  return perform_query('/dbfs/close', headers=headers, data=_data)

def put_file(src_path, dbfs_path, overwrite, headers):
  handle = create(dbfs_path, overwrite, headers=headers)['handle']
  print("Putting file: " + dbfs_path)
  with open(src_path, 'rb') as local_file:
    while True:
      contents = local_file.read(2**20)
      if len(contents) == 0:
        break
      add_block(handle, base64.standard_b64encode(contents).decode(), headers=headers)
    close(handle, headers=headers)

def put_file_from_url(url, dbfs_path, overwrite, headers):
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        handle = create(dbfs_path, overwrite, headers=headers)['handle']
        print("Putting file: " + dbfs_path)
        for i in range(0, len(content), 2**20):
            add_block(handle, base64.standard_b64encode(content[i:i+2**20]).decode(), headers=headers)
        close(handle, headers=headers)
        print(f"File {dbfs_path} uploaded successfully.")
    else:
        print(f"Error downloading file from {url}. Status code: {response.status_code}")

# Usage
create(path="dbfs:/FileStore/nlp/test_times.csv", overwrite= True, headers=headers)

# Usage
serve_times_url = "https://github.com/fivethirtyeight/data/blob/master/tennis-time/serve_times.csv?raw=true"
serve_times_dbfs_path = "dbfs:/FileStore/nlp/test_times.csv"
overwrite = True  # Set to False if you don't want to overwrite existing file

put_file_from_url(serve_times_url, serve_times_dbfs_path, overwrite, headers=headers)




# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/nlp/'))


print(os.path.exists('dbfs:/FileStore/nlp'))


# COMMAND ----------

# Initialize SparkSession
spark = SparkSession.builder.appName("Read CSV").getOrCreate()

# Define the file paths
test_times_path = "dbfs:/FileStore/nlp/test_times.csv"

# Read the CSV files into DataFrames
test_times_df = spark.read.csv(test_times_path, header=True, inferSchema=True)

# Show the DataFrames
test_times_df.show()
num_rows = test_times_df.count()
print(num_rows)
# save as delta
# transform_load
test_times_df.write.format("delta").mode("overwrite").saveAsTable("serve_times_delta")

# COMMAND ----------

people_df = spark.read.table("serve_times_delta")
people_df.show()
num_rows = people_df.count()
print(num_rows)
# query once you have both tables 
# merge them
# visuzalize them 
# save them as output 
# figure out how to run databrick 

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/nlp/'))

# COMMAND ----------

"""
Extract a dataset from a URL like Kaggle or data.gov. 
JSON or CSV formats tend to work well
"""
import requests
from pyspark.sql import SparkSession


FILESTORE_PATH = "dbfs:/FileStore/mini_project11"
headers = {'Authorization': 'Bearer %s' % access_token}


def extract(
    url="""
    https://github.com/fivethirtyeight/data/blob/master/tennis-time/serve_times.csv?raw=true 
    """,
    url2="""
    https://github.com/fivethirtyeight/data/blob/master/tennis-time/events_time.csv?raw=true
    """,
    file_path=FILESTORE_PATH+"/serve_times.csv",
    file_path2=FILESTORE_PATH+"/event_times.csv",
    directory=FILESTORE_PATH,
):
    """Extract a url to a file path"""
    # make the directory, no need to check if it exists or not
    mkdirs(path=directory, headers=headers)
    # add the csv files, no need to check if it exists or not
    put_file_from_url(url, file_path, overwrite, headers=headers)
    put_file_from_url(url2, file_path2, overwrite, headers=headers)

    return file_path, file_path2


# COMMAND ----------

extract()

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/mini_project11'))

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

def load(dataset="dbfs:/FileStore/mini_project11/event_times.csv", dataset2="dbfs:/FileStore/mini_project11/serve_times.csv"):
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()
    # load csv and transform it by inferring schema 
    event_times_df = spark.read.csv(dataset, header=True, inferSchema=True)
    serve_times_df = spark.read.csv(dataset2, header=True, inferSchema=True)

    # add unique IDs to the DataFrames
    event_times_df = event_times_df.withColumn("id", monotonically_increasing_id())
    serve_times_df = serve_times_df.withColumn("id", monotonically_increasing_id())

    # drop table if exists
    spark.sql("DROP TABLE IF EXISTS serve_times_delta")

    spark.sql("DROP TABLE IF EXISTS event_times_delta")


    # transform into a delta lakes table and store it 
    serve_times_df.write.format("delta").mode("overwrite").saveAsTable("serve_times_delta")
    event_times_df.write.format("delta").mode("overwrite").saveAsTable("event_times_delta")
    
    num_rows = serve_times_df.count()
    print(num_rows)
    num_rows = event_times_df.count()
    print(num_rows)
    
    return "finished transfrom and load"


    

# COMMAND ----------

load()

# COMMAND ----------

spark = SparkSession.builder.appName("Query").getOrCreate()

query_result = spark.sql("""
    SELECT 
        t1.id, t1.server, t1.seconds_before_next_point, t1.day, t1.opponent, t1.game_score, t1.set, t1.game,
        t2.tournament, t2.surface, t2.seconds_added_per_point, t2.years,
        COUNT(*) as total_rows
    FROM 
        serve_times_delta t1
    JOIN 
        event_times_delta t2 
        ON t1.id = t2.id
    GROUP BY 
        t1.id, t1.server, t1.seconds_before_next_point, t1.day, t1.opponent, t1.game_score, t1.set, t1.game,
        t2.tournament, t2.surface, t2.seconds_added_per_point, t2.years
    ORDER BY 
        t1.id, t2.years DESC, t1.seconds_before_next_point;
""")
query_result.show()


# COMMAND ----------

with open("sample_query.sql", "r") as file:
    sql_query = file.read()
    query_result = spark.sql(sql_query)
    query_result.show()


# COMMAND ----------

def query_transform():
    """
    Run a predefined SQL query on a Spark DataFrame.

    Returns:
        DataFrame: Result of the SQL query.
    """
    spark = SparkSession.builder.appName("Query").getOrCreate()
    query = """
        SELECT 
            t1.id, t1.server, t1.seconds_before_next_point, t1.day, t1.opponent, t1.game_score, t1.set, t1.game,
            t2.tournament, t2.surface, t2.seconds_added_per_point, t2.years,
            COUNT(*) as total_rows
        FROM 
            serve_times_delta t1
        JOIN 
            event_times_delta t2 
            ON t1.id = t2.id
        GROUP BY 
            t1.id, t1.server, t1.seconds_before_next_point, t1.day, t1.opponent, t1.game_score, t1.set, t1.game,
            t2.tournament, t2.surface, t2.seconds_added_per_point, t2.years
        ORDER BY 
            t1.id, t2.years DESC, t1.seconds_before_next_point
    """
    query_result = spark.sql(query)
    return query_result


# COMMAND ----------

query = query_transform()

# COMMAND ----------

query['seconds_before_next_point']

# COMMAND ----------

!pip install pyspark_dist_explore

# COMMAND ----------

import matplotlib.pyplot as plt
from pyspark_dist_explore import Histogram, hist


# Create a scatter plot using a histogram (since scatter plots are not directly supported in Spark)
fig, ax = plt.subplots()
hist(ax, query.select('seconds_before_next_point'), bins=20, color=['blue'])
ax.set_xlabel('Seconds Before Next Point')
ax.set_ylabel('Frequency')
ax.set_title('Scatter Plot: Seconds Before Next Point')
plt.show()



# COMMAND ----------

# Import necessary libraries
import matplotlib.pyplot as plt

# Create a box plot of 'seconds_before_next_point' by 'server'
plt.figure(figsize=(15, 8))  # Adjusted figure size
boxplot = query.select('seconds_before_next_point', 'server').toPandas().boxplot(column='seconds_before_next_point', by='server')
plt.xlabel('Server')
plt.ylabel('Seconds Before Next Point')
plt.suptitle('')
plt.title('Seconds Before Next Point by Server')
# Adjust the rotation and spacing of x-axis labels
plt.xticks(rotation=30, ha='right')  # ha='right' aligns the labels to the right
plt.tight_layout()  # Ensures proper spacing

plt.show()




# COMMAND ----------

average_seconds_by_surface = query.groupBy('surface').avg('seconds_before_next_point')

df_surface_avg = average_seconds_by_surface.toPandas()

# Create a bar chart
plt.figure(figsize=(10, 6))
plt.bar(df_surface_avg['surface'], df_surface_avg['avg(seconds_before_next_point)'], color='blue')
plt.xlabel('Surface Type')
plt.ylabel('Average Seconds Before Next Point')
plt.title('Average Seconds Before Next Point by Surface Type')
plt.xticks(rotation=45)
plt.show()


# COMMAND ----------

# sample viz for project
def viz():
    query = query_transform()
    plt.figure(figsize=(15, 8))  # Adjusted figure size
    boxplot = query.select('seconds_before_next_point', 'server').toPandas().boxplot(column='seconds_before_next_point', by='server')
    plt.xlabel('Server')
    plt.ylabel('Seconds Before Next Point')
    plt.suptitle('')
    plt.title('Seconds Before Next Point by Server')
    # Adjust the rotation and spacing of x-axis labels
    plt.xticks(rotation=30, ha='right')  # ha='right' aligns the labels to the right
    plt.tight_layout()  # Ensures proper spacing
    plt.savefig('server.png')  





# COMMAND ----------

viz()

# COMMAND ----------

import requests
from dotenv import load_dotenv

load_dotenv()
access_token = os.getenv("ACCESS_TOKEN")
job_id = os.getenv("JOB_ID")
server_h = os.getenv("SERVER_HOSTNAME")

url = f'https://{server_h}/api/2.0/jobs/run-now'

headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json',
}

data = {
    'job_id': job_id
}

response = requests.post(url, headers=headers, json=data)

if response.status_code == 200:
    print('Job run successfully triggered')
else:
    print(f'Error: {response.status_code}, {response.text}')

