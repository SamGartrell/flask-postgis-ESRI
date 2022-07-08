"""
Contains Secrets!

The main script for this app's backend. Presently an example run-through of the process

Pulls functions from the two other scripts in the repo to
 - Retrieve data from arcgis servers into a local folder
 - Convert that data into geopandas geodataframe objects
 - Perform analysis on those geodataframes
 - Load those geodataframes as tables into a postgis database

Package Dependencies:
 - geopandas
"""

import etl_functions as e
import calc_functions as c
import geopandas as gpd

# retrieve data from a particular arcgis server, store in './temp_geojsons'
e.ags_to_dir('services5.arcgis.com/7nsPwEMP38bSkCjy',
           0, 'geojson', 2000, 'FeatureServer', './temp_geojsons')

# convert to a geopandas geodataframe (d) and clear the directory
d = e.geojson_to_gpd('./temp_geojsons', clear_after=True)

# calculations will occur here with functions from the calc_functions script


# initialize variables for connection to postgres
user = 'postgres'
pw = 'Only3Follicles'
host = 'localhost'
port = '' # no port bc localhost
db = 'bwa_1'

schema = 'public'

# generate db url (u)
# u = e.mk_postgis_url(user, pw, host, db)
u = f'postgresql://{user}:{pw}@{host}/{db}'

# create postgres engine
engine = e.mk_postgis_engine(u)

# push data to postgres
d.to_postgis('taxlots', engine, schema, if_exists='replace')
