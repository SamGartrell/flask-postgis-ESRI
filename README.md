#Backend for the Brick + Work App

Architectural overview and instructions for local development


##Basic Structure

Primary backend script is `etl.py`:
  -Uses functions from `etl_functions.py` to retrieve and process geojson data from ArcGIS Servers into workable geopandas dataframes.
  -Uses functions from `calc_functions.py` to develop attributes based on real estate business logic.
  -Uses functions from `etl_functions.py` to send processed geopandas dataframes to the postgres database.


##Environment Setup

The backend's current package dependencies are outlined in the `env.yml` file.
Running the following in a CLI creates a compatible python environment from the file:
```
$ conda env create -f env.yml python==3.9.12
$ conda activate bwa_env
```

