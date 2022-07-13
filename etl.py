"""
basic etl process.
draws functions from etl_functions.py and parameters from etl_params.py
currently contains an example usage of ETL functions and parameters
"""

import etl_functions as ef
import etl_params as ep
import calc_functions as cf

# initialize db dictionary (from etl_params.py)
db = ep.postgres1

# initialize datasource dictionary (from etl_params.py)
ds = ep.zoning

# get some data from an arcgis rest endpoint
d = ef.ags_to_gdf(ds)

# perform analysis with calc_functions

# make a postgres engine and push data there
engine = ef.mk_postgis_engine(db)
d.to_postgis(db['table'], engine, db['schema'], if_exists=db['if_exists']) # this is a geopandas method

print(f'successful transmission to postgres\
        \n\ttable: {db["table"]}\
        \n\tschema: {db["schema"]}\
        \n\tdb: {db["database"]}')

print('commencing replacement')

# refresh the row where OBJECTID == 10, pulling the new values from the same ags endpoint as before
d2 = ef.oids_to_gdf(ds, [10])

# using the dataframe created above, perform a SQL update on the DB.
ef.update_with_gdf(db, db['table'], d2) # note that the table parameter is exposed alongside the db object parameter

print('done')