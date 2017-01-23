###############################################
# CaseStudyOSM_part_1.py
#
###############################################
# Combined Python Code to be added as .py files
# Part 1 of 2
# Part 1 performs all functions up to and including the creation of the .json file
#
# This code was updated to perform all cleaning updates in the .json shaping function

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


def count_tags(filename): 
# taken from Udacity Data Wrangling Course
    tag_names = defaultdict(int)
    for (event, elem) in iterparse(filename, ['start']): 
        tag_names[elem.tag] += 1
    return dict(tag_names)

def audit_street_type(street_types, street_name):
# taken from Udacity Data Wrangling Course
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)

def is_street_name(elem):
# taken from Udacity Data Wrangling Course
    return (elem.attrib['k'] == "addr:street")

def audit(osmfile):
# taken from Udacity Data Wrangling Course
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    osm_file.close()
    return street_types

def update_name(name, mapping):
# taken from Udacity Data Wrangling Course
    m = street_type_re.search(name)
    if m:
        s = name[:m.start()] + mapping[m.group()]
        print s
    return s

def process_map(file_in, pretty = False):
# taken from Udacity Data Wrangling Course
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data

#########################################################
# These functions performs the following:
#     reads the osm.xml file
#     shapes it for the .JSON Output
#     cleanse the data with the mapping and manual updates
#
def get_zip(str):
# Accepts string with the dirty postal code
# Returns the cleaned postal code
    m = re.search(r'\d{5}$', str) # Finds 5-digit zip codes 
    if m:
        return m.group(0) # returns the cleaned 5 digit zipcode
    else:
        m = re.search(r'\d{5}', str) #Finds zip+4 zipcodes
        if m:
            return m.group(0) # returns the cleaned 5 digit zipcode
        else:
            print "zip error"

def shape_element(element):
# Acepts a type from the python elementtree library
# this function extracts data from the tags and subtags.  
# It cleans, and shapes the data into the following datastructure:
#   - a list of dictionaries called "created" with user data
#   - a list of dictionaries called "address" with address data
#   - a list of floats called "pos" with lat/lon data
#   - a list of 'node_refs' call node_refs
#   - other node data is stored as top-level dictionary objects
#   - the function returns the data structure node for writing to the json file

    node = {}
    if element.tag == "node" or element.tag == "way" :
        created = {}
        pos = [0,0]
        address = {}
        node_refs = []
        
        node['type'] = element.tag
        
        for a in element.attrib.keys():
            #print a, element.attrib[a]
            if a in CREATED:
                created[a] = element.attrib[a]
            elif a in ['lat', 'lon']:
                if a == 'lat':
                    pos[0] = float(element.attrib[a])
                elif a == 'lon':
                    pos[1] = float(element.attrib[a])
                else:
                    print 'LAT/LON error'
            else:
                node[a] = element.attrib[a]
                                    
        for subtag in element:
            s = subtag.get('k')
            if ((s is None) == False):
                if (problemchars.search(s) is None):
                    keys = s.split(':')
                    if (keys[0] == 'addr'):
                        if (len(keys) == 2):
                            if keys[1] == "street":
                                o_street = subtag.get('v')  # Get 'original' street name
                                m = street_type_re.search(o_street) # m is the match object
                                if o_street == 'Journal Square #1204':
                                    address[keys[1]] = 'Journal Square' # Delete suite
                                elif o_street == '16th Street # 3':
                                    address[keys[1]] = '16th Street' # Delete suite
                                elif o_street == '2nd Street #C': 
                                    address[keys[1]] = '2nd Street' # Delete suite
                                elif o_street == 'US 1 (NJ)': 
                                    address[keys[1]] = 'US 1' # Delete suite
                                elif m:
                                    if m.group() in mapping.keys():  # Is it a mapped street type?
                                        n_street = o_street[:m.start()] + mapping[m.group()]
                                        address[keys[1]] = n_street # store the new street name    
                                else:
                                    address[keys[1]] = o_street
                            elif keys[1] == "postcode":
                                address[keys[1]] = get_zip(subtag.get('v'))  # Clean Postal Code
                            else:
                                address[keys[1]] = subtag.get('v')
                    elif (len(keys) == 1):
                        node[keys[0]] = subtag.get('v')
                    elif (len(keys) == 2):
                        node[keys[0]] = {keys[1] : subtag.get('v')}
            if subtag.tag == 'nd':
                node_refs.append(subtag.get('ref'))
        if created:
            node['created'] = created
        if address:
            node['address'] = address
        if pos != [0,0]:
            node['pos'] = pos
        if node_refs:
            node['node_refs'] = node_refs
        return node

    else:
        return None
         
#################################################
# Define Global Variables
#
osmfile = 'OSM_JerseyCityHoboken.xml'
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons", "Center", "Highway", "Plaza", "Turnpike", "Walk",
            "Walkway", "Way", "East", "Hudson", "North", "South"] # The last 5 are odd, but correct
miscoded_house_numbers = ["1204", "3", "C"] # Update in a future stage
# Mapping for First Stage Clensing
mapping = { "St": "Street",
            "St.": "Street",
            "Ave" : "Avenue",
            "Rd." : "Road",
           "Blvd" : "Boulevard",
           "Ctr" : "Center",
           "Clinton" : "Clinton Street",
           "1st" : "1st Street"}
dict_all = audit(osmfile)
dict_map = {}
lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]

#################################################
# Iterate through the element tree and put each tag and quantity in a dictionary
print
print "Count of each tag type"
pprint(count_tags(osmfile))
print
print "First Stage Street Type Mappings:"
print
for key, value in dict_all.iteritems():
    if key in mapping:
        dict_map[key] = dict_all[key]
pprint(dict_map)
print

data = process_map(osmfile, True)       