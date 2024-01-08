# Databricks notebook source
# MAGIC %sh
# MAGIC curl -G "https://opendata.fmi.fi/wfs" \
# MAGIC --data-urlencode "service=WFS" \
# MAGIC --data-urlencode "version=2.0.0" \
# MAGIC --data-urlencode "request=getFeature" \
# MAGIC --data-urlencode "storedquery_id=fmi::forecast::harmonie::surface::point::multipointcoverage" \
# MAGIC --data-urlencode "place=helsinki"

# COMMAND ----------

import requests
import xml.etree.ElementTree as ET

# API endpoint URL
url = "https://opendata.fmi.fi/wfs"

# Parameters for API request
params = {
    "service": "WFS",
    "version": "2.0.0",
    "request": "getFeature",
    "storedquery_id": "fmi::forecast::harmonie::surface::point::multipointcoverage",
    "place": "helsinki",
    "place": "espoo",
    "date": "27-12-2023",
}

# Make the API request
response = requests.get(url, params=params)

# Handle the response
if response.status_code == 200:
    # Parse the XML response
    root = ET.fromstring(response.content)

    # Find the desired tag
    namespace = {"gml": "http://www.opengis.net/gml/3.2"}
    data_block = root.find(".//gml:doubleOrNilReasonTupleList", namespace)

    # Find the schema block
    namespace_schema = {
    'gmlcov': 'http://www.opengis.net/gmlcov/1.0',
    'swe': 'http://www.opengis.net/swe/2.0'
    }

    schema_block = root.find('.//swe:field', namespace_schema)

    if data_block is not None:
        # Extract the data
        raw_data = data_block.text
        print("Data found")
        print(schema_block)
    else:
        print("Data not found in XML")

        
else:
    print("Error:", response.status_code)

# COMMAND ----------


data_rows = [row.split() for row in raw_data.strip().split("\n")]

# COMMAND ----------

df = spark.createDataFrame(data_rows)

display(df)

# COMMAND ----------

from pyspark.sql.functions import col
import requests
import xml.etree.ElementTree as ET

# API endpoint URL
url = "https://opendata.fmi.fi/wfs"

# Parameters for API request
params = {
    "service": "WFS",
    "version": "2.0.0",
    "request": "getFeature",
    "storedquery_id": "fmi::forecast::harmonie::surface::point::multipointcoverage",
    "place": "helsinki"
}

# Make the API request
response = requests.get(url, params=params)

root = ET.fromstring(response.content)

# Find the schema block
namespace = {
    'gmlcov': 'http://www.opengis.net/gmlcov/1.0',
    'swe': 'http://www.opengis.net/swe/2.0'
}
schema_block = root.find('.//swe:DataRecord', namespace)

# Extract the fields from the schema block
fields = []
if schema_block is not None:
    for field in schema_block.findall('swe:field', namespace):
        field_name = field.get('name')
        field_href = field.get('{http://www.w3.org/1999/xlink}href')
        fields.append((field_name, field_href))

# Print the extracted fields
for field_name, _ in fields:
    print("Field Name:", field_name)
    print()

# Extract field names
field_names = [field_name for field_name, _ in fields]

# Filter DataFrame based on extracted fields
filtered_df = df.select(*[col(field_name) for field_name in field_names])

# Display the filtered DataFrame
display(filtered_df)



# COMMAND ----------

import requests

# Replace these values with your actual Databricks workspace URL and personal access token
DATABRICKS_WORKSPACE_URL = "https://<databricks-workspace-url>"
DATABRICKS_TOKEN = "<your-personal-access-token>"

# Databricks REST API endpoint to list available clusters
API_ENDPOINT = f"{DATABRICKS_WORKSPACE_URL}/api/2.0/clusters/list"

# Set the headers with the authorization token
headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json",
}

# Make the REST API call
response = requests.get(API_ENDPOINT, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Print the response content (JSON format)
    print(response.json())
else:
    # Print an error message if the request was not successful
    print(f"Error: {response.status_code}, {response.text}")


# COMMAND ----------

import requests
import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# FMI WFS API endpoint URL
url = "https://opendata.fmi.fi/wfs"

# Parameters for API request
params = {
    "service": "WFS",
    "version": "2.0.0",
    "request": "getFeature",
    "storedquery_id": "fmi::forecast::harmonie::surface::point::multipointcoverage",
    "place": "helsinki"
}

# Make the API request
response = requests.get(url, params=params)

# Parse the XML response
root = ET.fromstring(response.content)

# Define namespaces for XML elements
namespace = {
    'gmlcov': 'http://www.opengis.net/gmlcov/1.0',
    'swe': 'http://www.opengis.net/swe/2.0'
}

# Extract the timestamp and temperature values from the XML response
timestamps = []
temperatures = []

for time_elem in root.findall('.//swe:time', namespace):
    timestamp = time_elem.text
    temperature_elem = time_elem.find('..//swe:quantity', namespace)
    if temperature_elem is not None:
        temperature = float(temperature_elem.text)
        timestamps.append(timestamp)
        temperatures.append(temperature)

# Create a Spark DataFrame
spark = SparkSession.builder.appName("FMIWeatherData").getOrCreate()
data = list(zip(timestamps, temperatures))
df = spark.createDataFrame(data, ["Timestamp", "Temperature"])

# Display the DataFrame
display(df)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import requests
import xml.etree.ElementTree as ET

# Create a Spark session
spark = SparkSession.builder.appName("FMIWeatherData").getOrCreate()

# First API request
url_first = "https://opendata.fmi.fi/wfs"
params_first = {
    "service": "WFS",
    "version": "2.0.0",
    "request": "getFeature",
    "storedquery_id": "fmi::forecast::harmonie::surface::point::multipointcoverage",
    "place": ["helsinki", "espoo"],  # Use a list for multiple places
    "date": "2023-12-27",
}

response_first = requests.get(url_first, params=params_first)

if response_first.status_code == 200:
    # Parse the XML response
    root_first = ET.fromstring(response_first.content)

    # Find the desired tag
    namespace_first = {"gml": "http://www.opengis.net/gml/3.2"}
    data_block_first = root_first.find(".//gml:doubleOrNilReasonTupleList", namespace_first)

    if data_block_first is not None:
        # Extract the data
        raw_data_first = data_block_first.text
        print("Data found in the first API response")
    else:
        print("Data not found in the first API response")
else:
    print("Error in the first API request:", response_first.status_code)


# Assuming data_rows_first is a list of lists obtained from the raw data in the first response
data_rows_first = [
    ["timestamp1", "temperature1"],
    ["timestamp2", "temperature2"],
    # ... more rows ...
]

# Create a PySpark DataFrame from the first set of data
df_first = spark.createDataFrame(data_rows_first, ["Timestamp", "Temperature"])

# Display the DataFrame
display(df_first)


# Second API request
url_second = "https://opendata.fmi.fi/wfs"
params_second = {
    "service": "WFS",
    "version": "2.0.0",
    "request": "getFeature",
    "storedquery_id": "fmi::forecast::harmonie::surface::point::multipointcoverage",
    "place": "helsinki",
}

response_second = requests.get(url_second, params=params_second)

if response_second.status_code == 200:
    # Parse the XML response
    root_second = ET.fromstring(response_second.content)

    # Find the schema block
    namespace_second = {
        'gmlcov': 'http://www.opengis.net/gmlcov/1.0',
        'swe': 'http://www.opengis.net/swe/2.0'
    }
    schema_block_second = root_second.find('.//swe:DataRecord', namespace_second)

    # Extract the fields from the schema block
    fields_second = []
    if schema_block_second is not None:
        for field_second in schema_block_second.findall('swe:field', namespace_second):
            field_name_second = field_second.get('name')
            field_href_second = field_second.get('{http://www.w3.org/1999/xlink}href')
            fields_second.append((field_name_second, field_href_second))

        # Print the extracted fields from the second API response
        for field_name_second, _ in fields_second:
            print("Field Name from the second API response:", field_name_second)
            print()

        # Extract field names
        field_names_second = [field_name_second for field_name_second, _ in fields_second]

        # Filter DataFrame based on extracted fields
        filtered_df_second = df_first.select(*[col(field_name_second) for field_name_second in field_names_second])

        # Display the filtered DataFrame from the second API response
        display(filtered_df_second)
    else:
        print("Schema block not found in the second API response")
else:
    print("Error in the second API request:", response_second.status_code)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import requests
import xml.etree.ElementTree as ET

# Create a Spark session
spark = SparkSession.builder.appName("FMIWeatherData").getOrCreate()

# Replace this with actual data from the first API response
data_rows_first = [
    ["2023-12-27T12:00:00", 25.5, 1012.3],
    ["2023-12-27T13:00:00", 26.2, 1011.8],
    # ... more rows ...
]

# Create a PySpark DataFrame from the first set of data
df_first = spark.createDataFrame(data_rows_first, ["Timestamp", "Temperature", "Pressure"])

# Display the DataFrame
display(df_first)

# API endpoint URL
url_second = "https://opendata.fmi.fi/wfs"

# Second API request parameters
params_second = {
    "service": "WFS",
    "version": "2.0.0",
    "request": "getFeature",
    "storedquery_id": "fmi::forecast::harmonie::surface::point::multipointcoverage",
    "place": "helsinki",
}

# Make the second API request
response_second = requests.get(url_second, params=params_second)

if response_second.status_code == 200:
    # Parse the XML response
    root_second = ET.fromstring(response_second.content)

    # Find the schema block
    namespace_second = {
        'gmlcov': 'http://www.opengis.net/gmlcov/1.0',
        'swe': 'http://www.opengis.net/swe/2.0'
    }
    schema_block_second = root_second.find('.//swe:DataRecord', namespace_second)

    # Extract the fields from the schema block
    fields_second = []
    if schema_block_second is not None:
        for field_second in schema_block_second.findall('swe:field', namespace_second):
            field_name_second = field_second.get('name')
            field_href_second = field_second.get('{http://www.w3.org/1999/xlink}href')
            fields_second.append((field_name_second, field_href_second))

        # Print the extracted fields from the second API response
        for field_name_second, _ in fields_second:
            print("Field Name from the second API response:", field_name_second)
            print()

        # Extract field names
        field_names_second = [field_name_second for field_name_second, _ in fields_second]

        # Filter DataFrame based on extracted fields
        filtered_df_second = df_first.select(*[col(field_name_second) for field_name_second in field_names_second])

        # Display the filtered DataFrame from the second API response
        display(filtered_df_second)
    else:
        print("Schema block not found in the second API response")
else:
    print("Error in the second API request:", response_second.status_code)


# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession
import requests

# Create a Spark session
spark = SparkSession.builder.appName("WebsiteData").getOrCreate()

# API endpoint URL (replace with the actual API endpoint)
api_url = "https://jsonplaceholder.typicode.com/todos/1"

# Make the API request
response = requests.get(api_url)

# Handle the response
if response.status_code == 200:
    # Assuming the response content is in JSON format
    json_data = response.json()

    # Convert the JSON data to a PySpark DataFrame
    df = spark.read.json(spark.sparkContext.parallelize([json_data]))

    # Display the DataFrame
    display(df)

    # You can further process and analyze the data as needed
    # For example, you might want to perform transformations or aggregations
    # df_transformed = df.select("desired_column").filter("some_condition")

    # Display the transformed DataFrame
    # display(df_transformed)
else:
    print("Error:", response.status_code)

