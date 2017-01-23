###############################################
# CaseStudyOSM_part_2.py
#
###############################################
# Combined Python Code to be added as .py files
# Part 2 of 2
# Part 2 performs all pymogo querries and performs the data visualizations
# 
# Important:
#      - Please ensure 'mongod' is running.
#      - The .json file has been imported to mongoDB
#      - mongoimport --db nj_osm --collection osm --type json --file OSM_JerseyCityHoboken.xml.json
#
%matplotlib inline
from collections import defaultdict
import xml.etree.cElementTree as ET
from xml.etree.ElementTree import iterparse
from pprint import pprint
import re
import codecs
import json
import pandas as pd
from IPython.display import display
import matplotlib.cm as cm
import seaborn as sns
import numpy as np
from mpl_toolkits.basemap import Basemap
from pymongo import MongoClient
import pymongo
import matplotlib.pyplot as plt
import numpy as np


client=MongoClient("localhost", 27017)
db = client.nj_osm
coll = db.osm



osmfile = 'OSM_JerseyCityHoboken.xml'

###############################################
# Validate Corrected Street Names
#
result =  coll.aggregate([{"$group":{ "_id":"$address.street", "count":{"$sum":1},
                                    'street_set' : {'$addToSet' : '$address.street'}}},
                          {'$match' : {'_id': {'$ne' : None } }},
                          {'$project' : {'_id' : '$street_set', 'count' : '$count'}},
                          {'$sort' : {'count' : -1}}                          
                        ])
print
print "Validate Corrected Street names"
print
display(pd.DataFrame(list(result)))
#
###############################################
# Validate Corrected Postal Codes 
#
result =  coll.aggregate([{"$group":{ "_id":"$address.postcode", "count":{"$sum":1},
                                    'zip_set' : {'$addToSet' : '$address.postcode'}}},
                          {'$match' : {'_id': {'$ne' : None } }},
                          {'$project' : {'_id' : '$zip_set', 'count' : '$count'}},
                          {'$sort' : {'count' : -1}}                          
                        ])
print
print "Validate Corrected Postal Codes"
print
display(pd.DataFrame(list(result)))
#
###############################################
# Find the Total Size of the Dataset
#
result = coll.find().count()
print
print "Total Size ="
print
pprint(result)
#
#######################################################
# Investigate the quantities of each document 'type' in the Dataset
#
result =  coll.aggregate([{"$group":{ "_id":"$type", "count":{"$sum":1}}},
                       {"$sort" : {"count" : -1}},
                       {"$limit" : 10}])
# Show quantities of each document 'type' in the Dataset
print
print "Quantities of each document type"
print
display(pd.DataFrame(list(result)))
#
######################################################
# Find the number of unique users
#
result =  coll.aggregate([{"$group":{ "_id":"$created.user", 
                                         'u_set' : {'$addToSet' : '$created.user'}}}])
print
print "Number of unique users:"
print
display(pd.DataFrame(list(result)))
#
######################################################
# List the top 10 OSM users with the most contributions
#
result =  coll.aggregate([{"$group":{ "_id":"$created.user", "count":{"$sum":1}}},
                       {"$sort" : {"count" : -1}},
                       {"$limit" : 10}])
print
print "top 10 OSM users with the most contributions:"
print len(list(result))
print
#
######################################################
# List the Top 10 Amenities by Quantity
#
result =  coll.aggregate([ {'$match' : {'amenity' : {'$ne' : None}}},
                        {"$group":{ "_id":"$amenity", "count":{"$sum":1}}},
                        {"$sort" : {"count" : -1}},
                        {"$limit" : 10}])
print
print "List the Top 10 Amenities by Quantity"
display(pd.DataFrame(list(result)))
print
#######################################################
# List Parking by Name
#
result =  coll.aggregate([ {'$match' : {'amenity' : "parking"}},
                        {"$group":{ "_id": "$name","count":{"$sum":1}}},
                        {"$sort" : {"count" : -1}},
                        {"$limit" : 10}])
print
print "List Parking by Name"
display(pd.DataFrame(list(result)))
print
#
#######################################################
# List Places of Worship by Religion
#
result =  coll.aggregate([ {'$match' : {'amenity' : "place_of_worship", 'religion' : {'$ne' : None}}},
                        {"$group":{ "_id": "$religion","count":{"$sum":1}}},
                        {"$sort" : {"count" : -1}},
                        ])
print
print "List Places of Worship by Religion"
display(pd.DataFrame(list(result)))
print
#
#######################################################
# List Restaurants by Name
#
result =  coll.aggregate([ {'$match' : {'amenity' : "restaurant"}},
                        {"$group":{ "_id": "$name","count":{"$sum":1}}},
                        {"$sort" : {"count" : -1}},
                        {"$limit" : 10}])
print
print "List Restaurants by Name"
display(pd.DataFrame(list(result)))
print
#
#######################################################
# List Fast Food by Name
#
result =  coll.aggregate([ {'$match' : {'amenity' : "fast_food"}},
                        {"$group":{ "_id": "$name","count":{"$sum":1}}},
                        {"$sort" : {"count" : -1}},
                        {"$limit" : 10}])
print
print "List Fast Food by Name"
display(pd.DataFrame(list(result)))
#
#######################################################
# List Hospital by Name
#
result =  coll.aggregate([ {'$match' : {'amenity' : "hospital", 'name' : {'$ne' : None}}},
                        {"$group":{ "_id": "$name","count":{"$sum":1}}},
                        {"$sort" : {"count" : -1}},
                        ])
print
print "List Hospital by Name"
display(pd.DataFrame(list(result)))
print
#######################################################
# List Toilets by Name
#
result =  coll.aggregate([ {'$match' : {'amenity' : "toilets"}},
                        {"$group":{ "_id": "$name","count":{"$sum":1}}},
                        {"$sort" : {"count" : -1}},
                        ])
print
print "List Toilets by Name"
display(pd.DataFrame(list(result)))
print
#######################################################
# List Fire Stations by Name
#
result =  coll.aggregate([ {'$match' : {'amenity' : "fire_station"}},
                        {"$group":{ "_id": "$name","count":{"$sum":1}}},
                        {"$sort" : {"count" : -1}},
                        ])
print
print "List Fire Stations by Name"
display(pd.DataFrame(list(result)))
print
#######################################################
# List Amenties with POS Coordinates for Scatter Plot
#
result =  coll.aggregate([ {'$match' : {'pos' : {'$ne' : None}, 'amenity' : {'$ne' : None}}},
                        {"$group":{ "_id": "$amenity","count":{"$sum":1}}},
                        {"$sort" : {"count" : -1}},
                        {"$limit" : 10}])
print
print "List Amenties with POS Coordinates for Scatter Plot"
display(pd.DataFrame(list(result)))
print
display(pd.DataFrame(list(result)))
print
#########################################################
# Create Scatter Plot of Grouped Amenities
#
def assign_amenityID(str):
# this function groups amenities into groups and assigns an int group ID
# the grouping combines similar amenities and assign the ID to simplify plotting the data.
# The function accepts a string and returns an int
#
    if (str == 'parking') or (str == 'parking_space'):
        return 1
    elif (str == 'place_of_worship'):
        return 2
    elif (str == 'school'):
        return 3
    elif (str == 'restaurant') or (str == '	fast_food'):
        return 4
    elif (str == 'toilets'):
        return 5    
    elif (str == 'fire_station'):
        return 6
    else:
        return 0

label = {0 : 'Other', 1:'Parking', 2:'Worship', 3:'School', 4:'Restaurant', 5:'Toilet', 6:'Fire Station'}

result =  coll.aggregate([ {'$match' : {'pos' : {'$ne' : None}, 'amenity' : {'$ne' : None}}},
                        {'$project' : {'amenity' : '$amenity', 'pos' : '$pos', 'name' : '$name'}},
                        ])
df1 = pd.DataFrame(list(result)).set_index('_id')

df1['lat'] = df1['pos'].apply(lambda row: row[0]) # extract Lat and Lon from POS array
df1['lon'] = df1['pos'].apply(lambda row: row[1]) # extract Lat and Lon from POS array
df1['amenityID'] = df1['amenity'].apply(assign_amenityID)

groups = df1.groupby('amenityID')
fig = plt.figure()
ax = plt.subplot(111)
ax.margins(0.10) # Optional, just adds 5% padding to the autoscaling
for name, group in groups:
    ax.plot(group.lon, group.lat, marker='.', linestyle='', ms=9, label=label[name])

# Shrink current axis by 20%
box = ax.get_position()
ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])

# Put a legend to the right of the current axis
ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

print
print "Scatter Plot of Grouped Amenities - inline plot"
plt.show()
print
#
######################################################################################
# Create a Basemap plot
#
from mpl_toolkits.basemap import Basemap

m = Basemap(projection = 'mill',llcrnrlat = 40.6881, urcrnrlat = 40.7646,
            llcrnrlon = -74.2086, urcrnrlon = -74.0201, resolution = 'c')
# To get the basemap plot in the summary, change the resolutuin to 'f'
#
m.drawcoastlines(linewidth = 1)
m.drawcounties(color = 'r',linewidth = 1.0)
m.drawrivers(color = 'b')
m.fillcontinents(color='lightgreen', lake_color='#FFFFFF')
m.drawmapboundary(fill_color='#FFFFFF')

lats = df1['lat'].tolist()
lons = df1['lon'].tolist()
vals = df1['amenityID'].tolist()

for lon, lat, val in zip(lons, lats, vals):
    x,y = m(lon, lat)
    m.plot(x, y, '.', markersize = 8, alpha = 1)

plt.title('Jersey City / Hoboken Study OSM Area w/ Amenities')
print
print "Basemap Plot of Grouped Amenities - inline plot"
print
plt.show
