
# coding: utf-8

# # MASSIVE DATA PROCESSING
# 
# ## BIG DATA PROJECT
# 
# ### IDEALISTA - TOP N CITIES BY DESIRED PROPERTY
# 
# #### Group:
# Daniel Vieira Cordeiro
# 
# Guillem Orellana Trullols
# 
# Marc Viladegut Abert
# 
# 
# 
# #### Coordination:
# CORES PRADO, FERNANDO
# 
# MATEU PIÑOL, CARLOS
# 
# 
# #### Description:
# 
# Using the previouly gathered and cleaned dataset containing the information of properties on the idealista website, by making use of Apache Spark and Python, we created this notebook that filters properties by:
# 
# - Maximum rent price the user is willing to pay 
# - Minimum number of rooms the user seeks in his new home
# - If the annoucement has photos of the property
# - The property type: "Flat, House, etc.."
# 
# Once it has the data filtered, it generates a list of TOP N Cities containing the properties that match the requirements.

# In[2]:


# Import useful libraries
import pyspark, string, json, argparse

# Creating Spark Context
sc = pyspark.SparkContext()
print(sc)

# Reading cleaned Idealist properties file
parser = argparse.ArgumentParser()
parser.add_argument("input", help="Input data")
parser.add_argument("output", help="output data")
args = parser.parse_args()

clean_data = sc.textFile(args.input)
print("Header:")
print(clean_data.first())

# Get properties data fields (string split separate by ,)
data_header = clean_data.map(lambda line: line.split(","))

# Separating Header from data
header = data_header.first()
data = data_header.filter(lambda row: row != header)

print("\nInitial Data:")
print(data.take(3))

# Filtering Columns
simple_data = data.map(lambda r: (r[2], r[4], r[5], r[9], r[-1]))

print("\nInitial Data - Important Columns Only:")
print(simple_data.take(3))

# Setting up filters
maxPrice = 1000000.0
minRooms = 4
minPhotos = 0
houseType = "flat"

# Filtering Data
filtered = simple_data.filter(lambda r: int(r[0]) >= minPhotos and float(r[1]) < maxPrice and r[2] == houseType and int(r[3]) >= minRooms)

print("\nFiltered Data:")
print(filtered.take(3))

# Defining number of Top Cities
N = 8

# Keeping only the City info and a value = 1
cities = filtered.map(lambda h: (h[4],1))

# Printing the total number of Cities and how many distincts are there
print("\nTotal number of Cities:  " + str(cities.count()))
print("\nTotal number of Distinct Cities:  " + str(cities.distinct().count()))

# Counting and joining hashtags (#)
property_count = cities.reduceByKey(lambda a, b: a + b)#.filter(lambda t: t[1])
print(property_count.take(N))

# Ordering decresingly by amount
properties_ordered = property_count.takeOrdered(N, key = lambda x: -x[1])
print("\nTop Cities with matching properties: \n")
print(properties_ordered)

# Converting the list to parallel RDD
parallel_properties_ordered = sc.parallelize(properties_ordered)

# Use the map function to write one element per line and write all elements to a single file (coalesce)
parallel_properties_ordered.coalesce(1).map(lambda row: str(row[0]) + " " + str(row[1])).saveAsTextFile(args.output)
