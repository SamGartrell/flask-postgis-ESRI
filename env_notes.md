# API Environment Creation Notes:

Because this environment requires packages from pypi (outside of conda), these notes will replace the environment.yml file. Instead, the final solution should use automated command line input to create the environment.

## Creation Steps:

create environment in current directory and step into it:
`conda create -p .\envname`
`conda activate .\envname`

add flask:
`conda install flask`

add flask-sqlalchemy:
`conda install flask-sqlalchemy`

add flask-migrate:
`conda install -c conda-forge flask-migrate`

add the standalone version of psycopg2:
`pip install psycopg2-binary`
