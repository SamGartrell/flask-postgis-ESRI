"""
created 6/2022 by Sam Gartrell

"""

import requests
from datetime import datetime
import json
import geojson
import glob
import os
import geopandas as gpd
import pandas as pd
import sqlalchemy as sa



def ags_to_dir(host: str, layerid: int, format: str, max_records: int, service_type: str, out_dir: str) -> None:
    """
    pages data from an arcgis rest server endpoint in geojson format,
    writes it to a specified directory
    """
    offset = 0
    iter = 0
    exceeded_limit = True

    # while exceeded_limit:  # for actual
    for i in range(2):  # for testing

        d = requests.get(
            f'https://{host}/arcgis/rest/services/TEST_PYTHON_ZONING/FeatureServer/{layerid}/query?outFields=*&where=1%3D1&f={format}&resultOffset={offset}')

        # verify HTTP status
        if d.status_code != 200:
            if offset > 0:
                raise Exception(
                    f'Unknown error. Last successful batch offset: {offset}.')
            else:
                raise ValueError('Incorrect url probably.')

        # increment offset and iterator
        offset += max_records
        iter += 1

        # initialize a dict variable for current geojson
        page = d.json()

        # add timestamp and append to current geojson, one feature at a time
        ts = datetime.timestamp(datetime.now())

        for feature in page['features']:
            feature['properties']['timestamp'] = ts

        # write the geojson in the temp_geojsons directory.
        gjson_name = f'batch{iter}.geojson'
        gjson_path = out_dir
        fin = os.path.join(gjson_path, gjson_name)

        with open(fin, 'w') as temp_json:
            json.dump(page, temp_json)

        print(f'round {iter}: {len(page["features"])} records added')

        # verify that there are more records to recieve before looping
        try:
            exceeded_limit = d.json()['properties']['exceededTransferLimit']
        except KeyError:
            exceeded_limit = False
    return None


def ags_to_gdf(host: str, svc:str, stype:str, layerid: int, format: str, rec_cap: int) -> None:
    """
    pages data from an arcgis rest server endpoint in geojson format,
    turns each page into its own GDF, then flattens them into one GDF
    """
    offset = 0
    iter = 0
    exceeded_limit = True
    gdf_list = []

    # while exceeded_limit:  # for actual
    for i in range(2):  # for testing

        d = requests.get(
            f'https://{host}/arcgis/rest/services/{svc}/{stype}/{layerid}/query?outFields=*&where=1%3D1&f={format}&resultOffset={offset}')

        # verify HTTP status
        if d.status_code != 200:
            if offset > 0:
                raise Exception(
                    f'Unknown error. Last successful batch offset: {offset}.')
            else:
                raise ValueError('Incorrect url probably.')

        # increment offset and iterator
        offset += rec_cap
        iter += 1

        # initialize a dict variable for current geojson
        page = d.json()

        # add timestamp and append to current geojson, one feature at a time
        ts = datetime.timestamp(datetime.now())

        for feature in page['features']:
            feature['properties']['timestamp'] = ts

        # make the dict into a gpd gdf
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


def list_contents(file_type:str, dir_name:str)->list:
    """
    modified on 6/26/22 from https://gist.github.com/ericrobskyhuntley/790937a759fd89ed77c8831f880f854c
    lists the contents of a specified directory.
    both input strings should start with a period.
    """
    pattern = os.path.join(dir_name, f'*{file_type}')
    file_list = glob.glob(pattern)

    return file_list

def compile_geojson(out_file_name=None, geojson_dir_name='./'):
    """
    modified on 6/26/22 from https://gist.github.com/ericrobskyhuntley/790937a759fd89ed77c8831f880f854c
    creates a single geojson from a directory of geojsons
    params:
        out_file_name (str): optional filename to write the geojson to
        geojson_dir_name (str): the dir containing subject geojsons
    
    returns:
        either None (if writing to file), or a geometry collection of all the geojsons
    """
    file_list = list_contents('.geojson', geojson_dir_name)
    collection = []

    for file in file_list:
        with open(file, 'r') as f:
            layer = geojson.load(f)
            collection.append(layer)

    geo_collection = geojson.GeometryCollection(collection)
    if out_file_name != None:
        with open(out_file_name, 'w') as f:
            geojson.dump(geo_collection, f)
        return None
    else:
        return geo_collection


def geojson_to_gpd(geojson_dir:str, clear_after=True,) -> gpd.GeoDataFrame:
    """
    reads every geojson contained in a specified directory, 
    converting them into a single geopandas geodataframe.
    params:
        geojson_dir (str): the directory to comb for geojsons
        clear_after (bool, default True): erase all contents of the directory
    """
    pages = list_contents('.geojson', geojson_dir)
    df_list = []

    # convert each geojson to a gpd dataframe
    for page in pages:
        new_gdf = gpd.GeoDataFrame.from_file(page)

        df_list.append(new_gdf)

    # report number of geojsons found
    print(f'{len(df_list)} geojsons read into gdf\n')

    # clear temporary geojson directory
    if clear_after:
        for file in os.scandir(geojson_dir):
            os.remove(file.path)

    # concatenate/flatten the dfs into a single gdf
    data = pd.concat(df_list, axis=0)

    return data


def mk_postgis_uri(usr:str, pw:str, host:str, port:str, db:str) -> str:
    """
    constructs a sequence of strings into a url for a postgres engine.
    params:
        usr: db username
        pw: db user pw
        host: the host containing the db
        port: the port the db is running on
        db: the name of the target db
        schema: the name of target schema
        encrypt: encrypt username and pw
    TODO: add encryption with urllib
    """

    if port != None:
       postgis_url = f'postgresql://{usr}:{pw}@{host}:{port}/{db}'
    else:
       postgis_url = f'postgresql://{usr}:{pw}@{host}/{db}'
       
    return postgis_url

def mk_postgis_engine(url:str):
    """
    makes a postgres engine using the url parameter
    (basically exists to avoid importing sqlalchemy in etl.py)
    """
    engine = sa.create_engine(url)
    return engine


