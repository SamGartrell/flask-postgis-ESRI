"""
created 6/2022 by Sam Gartrell
"""

import requests as r
from datetime import datetime as dt
import geopandas as gpd
import pandas as pd
import sqlalchemy as sa
import psycopg2 as ps
from psycopg2.extras import RealDictCursor


def ags_to_gdf(endpoint:dict, lyr_def:str=None, returngeo:str=None) -> gpd.GeoDataFrame:
    """
    pages data from an arcgis rest server endpoint in geojson format,
    turns each page into its own pandas geodataframe (gdf), then flattens them into one gdf

    dependencies: requests as r, datetime as dt, geopandas as gpd, pandas as pd
    """
    offset = 0
    iter = 0
    exceeded_limit = True
    gdf_list = []

    # create url
    if lyr_def:
        url = f'https://{endpoint["host"]}/arcgis/rest/services/{endpoint["service"]}/{endpoint["service_type"]}/{endpoint["layer_id"]}/query?{lyr_def}&outFields=*&f={endpoint["format"]}'
    else:
        url = f'https://{endpoint["host"]}/arcgis/rest/services/{endpoint["service"]}/{endpoint["service_type"]}/{endpoint["layer_id"]}/query?outFields=*&where=1%3D1&f={endpoint["format"]}'
    
    if returngeo:
        url += f'&returnGeometry={returngeo}'

    # get records from url
    # while exceeded_limit:  # for actual
    for i in range(4):  # for testing
        current_url = f'{url}&resultOffset={offset}'
        d = r.get(
            current_url
            )
        print(f'request made using: \n{current_url}')
            

        # verify HTTP status
        if d.status_code != 200:
            if offset > 0:
                raise Exception(
                    f'Unknown error. Last successful batch offset: {offset}.')
            else:
                raise ValueError('Incorrect url probably.')

        # initialize a dict variable for current geojson
        page = d.json()

        # additional HTTP error handling, just in case
        try:
            print(f'Error:{page["error"]}')
            exit()

        except:
            pass

        # increment offset and iterator after previous page is confirmed successful
        offset += endpoint["max_records"]
        iter += 1

        # add timestamp and append to current geojson, one feature at a time
        ts = dt.timestamp(dt.now())

        for feature in page['features']:
            feature['properties']['timestamp'] = ts

        # make the dict into a gdf and append it to a list (to be flattened later)
        p_gdf = gpd.GeoDataFrame.from_features(page)
        gdf_list.append(p_gdf)

        print(f'round {iter}: {len(page["features"])} records added')

        # verify that there are more records to recieve before looping
        try:
            exceeded_limit = d.json()['properties']['exceededTransferLimit']
        except KeyError:
            exceeded_limit = False
    
    # after pagination is complete, flatten the list of dataframes into one variable
    compiled_gdf = pd.concat(gdf_list, axis=0)

    print(f'\ngeodataframe created successfully')

    # return the compiled geodataframe
    return compiled_gdf

def mk_postgis_engine(database:dict, mk_engine:bool=True):
    """
    recieves a database dict, returns either an sqlalchemy engine object (if mk_engine==True),
    or just a database url (if mk_engine==False).

    dependencies: sqlalchemy as sa
    """
    if database["port"] not in (None, '', False):
       url = f'postgresql://{database["user"]}:{database["password"]}@{database["host"]}:{database["port"]}/{database["database"]}'
    else:
       url = f'postgresql://{database["user"]}:{database["password"]}@{database["host"]}/{database["database"]}'

    if mk_engine:
        engine = sa.create_engine(url)
        return engine
    else:
        return url


def oids_to_gdf(endpoint:dict, oid_list:list) -> gpd.GeoDataFrame:
    """
    recieves an endpoint dict and a list of OBJECTIDs;
    retrieves records with corresponsing ids and returns a geodataframe

    Note: doesn't support paging

    dependencies: requests as r, datetime as dt, geopandas as gpd
    """
    # create OBJECTID query string
    oid_def = 'objectIds='

    # check if the caller is trying to get all records, call the regular paging function if so
    if oid_list == ['all']:
        print('refreshing all records')
        return ags_to_gdf(endpoint)

    # add segments to OBJECTID query string for each sought ID
    for i in oid_list:
        oid_def += f'{i}%2C+'

    # remove trailing '%2c+' bits without .strip()
    oid_def = oid_def[:-4]

    # create url
    url = f'https://{endpoint["host"]}/arcgis/rest/services/{endpoint["service"]}/{endpoint["service_type"]}/{endpoint["layer_id"]}/query?{oid_def}&outFields=*&f={endpoint["format"]}'

    # make request to url
    d = r.get(url)
    print(f'request made using: \n{url}\n\n')
        

    # verify HTTP status
    if d.status_code == None:
        raise ValueError('Incorrect url probably.')
    elif d.status_code != 200:
        raise Exception(f'Error: {d.status_code}.')

    # initialize a dict variable for current record
    record = d.json()

    # additional HTTP error handling
    try:
        print(f'Error:{record["error"]}')
        exit()

    except:
        pass

    # add timestamp and append to current record, one feature at a time
    ts = dt.timestamp(dt.now())

    for feature in record['features']:
        feature['properties']['timestamp'] = ts

    # make the dict into a gpd gdf
    record_gdf = gpd.GeoDataFrame.from_features(record)

    # return the compiled geodataframe
    return record_gdf

def update_with_gdf(database:dict, table:str, gdf) -> None:
    """
    recieves a database dict and a pre-processed geodataframe;
    updates the database to reflect values of the input geodataframe.

    note: geodataframe must have records matching those in the table (i.e. must be
    from the same source and processed in the same way as existing records).

    package dependencies: psycopg2 as ps, geopandas as gpd
    """

    # initialize postgres connection
    con = ps.connect(
        host=database["host"],
        database=database["database"],
        user=database["user"],
        password=database["password"]
    )

    # create a cursor to read with
    cur = con.cursor()

    # initialize dataframe/SQL variables
    oids = list(gdf["OBJECTID"])  #list of the gdf's OBJECTIDs
    fields = list(gdf.columns.values)  #list of the gdf's fields
    update_exp = [] # empty list of expressions to run later


    # iterate through rows by oid, composing an SQL statement for each row and appending it to a list
    for id in oids:
        set_clause = 'SET' # start a "SET" clause for the statement

        for field in fields:
            if field in ("OBJECTID", 'geometry'): # ignore these fields bc they aren't expected to change
                pass

            else:
                val = list(gdf.query(f'OBJECTID == {id}')[field])[0] # get values at intersection of oid (row) and field (column)
                set_clause += f""" "{field}" = '{val}',""" # add SQL language to map the field and value

        set_clause = set_clause[:-1] # remove the trailing comma

        # after iterating through the row's fields, assemble the final SQL expression and add it to a list
        update_exp.append(f"""UPDATE {table} {set_clause} WHERE "OBJECTID" = {id};""")

    print(f'\nupdating OBJECTIDs {oids} in table "{table}"\n')

    for exp in update_exp:
        print(f'executing:\n{exp}')
        cur.execute(exp)
        

    print('\n\nfinished updating.')

    con.commit()
    cur.close()
    con.close()

def retrieve_from_postgis(database:dict, table:str, oid_list:list ) -> dict:
    """
    returns a dictionary of records retrieved from an input postgres database/table,
    corresponding to the input list of objectids (or a list containing a single string "all").

    dependencies: psycopg2, ps
    """
    # initialize a string to represent the in IN expression's array
    exp_array = ''
        
    # initialize a list to return records with
    out_array = []

    # initialize postgres connection
    con = ps.connect(
        host=database["host"],
        database=database["database"],
        user=database["user"],
        password=database["password"]
    )

    # create a cursor to read with (realdict cursor returns rows as dictionaries)
    cur = con.cursor(cursor_factory=RealDictCursor)

    if oid_list in (["all"], 'all'):
        exp = f'select * from {database["schema"]}.{table} order by "OBJECTID";'
    else:
        # iteratively add ids to the array string
        for id in oid_list:
            exp_array += f'{str(id)}, '

        # remove trailing ', ' from array string
        exp_array = exp_array[:-2]

        # put together the final expression string
        exp = f'select * from {database["schema"]}.{table} where "OBJECTID" in ({exp_array}) order by "OBJECTID";'

    print(f'executing:\n\t{exp}')

    # execute the select expression
    cur.execute(exp)

    # get query results as a list of rows
    res = cur.fetchall()

    # ditch the geometry field
    for record in res:
        rec = dict(record)
        del rec['geometry']
        out_array.append(rec)

    cur.close()
    con.close()

    return out_array