"""
basic etl process
"""

import etl_functions as e
import calc_functions as c
import geopandas as gpd

# routing params
host = 'services5.arcgis.com/7nsPwEMP38bSkCjy'  #break into machine, domain, webadaptor
service = 'MtLee_FireRoads_and_Trails'
service_type = 'FeatureServer'
layer_id = 0

# query params
format = 'geojson'
max_records = 2000

# retrieve data from a particular arcgis server, store in './temp_geojsons'
d_rd = e.ags_to_gdf(host, service, service_type,
           layer_id, format, max_records)


# # calculations will occur here with functions from the calc_functions script


# initialize variables for connection to postgres
user = 'postgres'
pw = 'Only3Follicles'
host = 'localhost'
port = '' # no port bc localhost
db = 'bwa_1'

schema = 'public'

# generate db uri (u)
# u = e.mk_postgis_uri(user, pw, host, db)
u = f'postgresql://{user}:{pw}@{host}/{db}'

# create postgres engine
engine = e.mk_postgis_engine(u)

# push data to postgres
d_rd.to_postgis('biz', engine, schema, if_exists='replace')