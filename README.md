# Backend with Python, Flask, and Postgis for ArcGIS Rest Data
Architectural overview and instructions for local development

## Basic Design
### Primary backend script is `etl.py`, with database and data source arguments derived from `etl_params.py` (not included in this repo):
 <br />   - Uses functions from `etl_functions.py` to retrieve and process geojson data from ArcGIS Servers into workable geopandas dataframes.
 <br />   - Uses functions from `calc_functions.py` to develop attributes based on real estate business logic.
 <br />   - Uses functions from `etl_functions.py` to send processed geopandas dataframes to the postgres database.
 <br />   - Eventually will run on a chron job as an executable, routinely refreshing database records.
 
 ### Flask API lives in `app.py`, and communicates through GET and POST requests:
 <br />   - `GET` requests inherit parameters from url routes. A `GET` request at the base route ('/') returns all records in the table, while a `GET` request at an `OBJECTID` route ('/<objectid value>') returns a single record corresponding with that `OBJECTID` value (or a `404` if the specified value doesn't exist in the table).
 <br />   - `POST` requsts trigger a refresh for specified database records. They must be formatted as `{"objectid":<value>}`, where `<value>` is either an array of length 1-1000 containing ids (`[1, 5, 10]` or `[1]`), or an array containing "all" (`["all"]`). The prior will refresh whatever values are listed, while the latter will refresh all records in the table (essentially a manually triggered ETL run).
 <br />   - NOTE: The table targeted by the API is presently hardcoded into the API, constrained by fields defined in the API's data model (Flask convention). The model presently aids in formulating get requests, but hopefully I'll eliminate that dependency down the road and make the API more flexible in database connections. This would increase points of contact between the API and the ETL scripts, relegating the database to a less active role.
