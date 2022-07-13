"""
Flask API 

retrieves records from the database and calls smaller ETL workflows to refresh records. 

GET requests hit the table specified in the db.Model object defined below, 
returning either all records in the table (at the base route '/') or just 
one record (at an objectid-specific route '/<objectid>'). 

POST requests are used to refresh records (which are then returned), and can access 
between 1 and 1000 records, or the entire table's worth (at the base route '/'). 
They accept a request JSON in the following structure, where <id values> is a list of ids (of length 
1-1000), or the string "all" (to refresh everything; only available at the base route '/'). 
    ```
    {
        "objectid" : [<id values>]
    }
    ```
"""

from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy as sa
from flask_migrate import Migrate as mi
import etl_functions as ef
import etl_params as ep

# initialize database dictionary
dbase = ep.postgres1 # not to be confused with variable 'db', a flask-sa instance

user = dbase['user']
pw = dbase['password']
host = dbase['host']
database = dbase['database']

# initialize data source dictionary
dsource = ep.zoning

# initialize app variables
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{user}:{pw}@{host}/{database}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# initialize flask-sqlalchemy and flask-migrate instances to manage db querying and transactions
db = sa(app)
migrate = mi(app, db)

# define a data model to teach flask the structure of postgres tables
# TODO: Update this with whatever table structure ur using (or create a function to automate)
class TaxlotModel(db.Model):
    __tablename__ = dbase['table']  # NOTE: makes table name dynamic, but fields still need to be manually configured 

    OBJECTID = db.Column(db.BigInteger, primary_key=True)
    ZONE_CMPLT = db.Column(db.Text)
    ZONE_CLASS = db.Column(db.Text)
    ZONE_SMRY = db.Column(db.Text)
    Shape__Area = db.Column(db.Float)
    Shape__Length = db.Column(db.Float) # ref: https://docs.sqlalchemy.org/en/14/core/type_basics.html#sqlalchemy.types.Float
    timestamp = db.Column(db.Float)
    # geometry column is ignored

    # initialize the data model 
    def __init__(self, OBJECTID, ZONE_CMPLT, ZONE_CLASS, ZONE_SMRY, Shape__Area, Shape__Length, timestamp):
        self.OBJECTID = OBJECTID
        self.ZONE_CMPLT = ZONE_CMPLT
        self.ZONE_CLASS = ZONE_CLASS
        self.ZONE_SMRY = ZONE_SMRY
        self.Shape__Area = Shape__Area
        self.Shape__Length = Shape__Length
        self.timestamp = timestamp

    def __repr__(self):
        return f'<Taxlot {self.OBJECTID}>'


@app.route('/', methods=['POST', 'GET'])
def handle_taxlots():  #for record creation and getting ALL records
    
    # POST requests at this route accept JSONS following the format {"objectid": [<values>]}
    # NOTE: values can either be a list of ids, or the string "all", which will refresh all records.
    if request.method == 'POST':
        if request.is_json:
            data = request.get_json()

            # if fieldname == objectid, query with the objectid-based function
            if "objectid" in data and len(data) == 1:

                # get a geodataframe with the records corresponding with requested objectids (doesn't support lists longer than 1000)
                gdf = ef.oids_to_gdf(dsource, data["objectid"])

                # TODO: Make functions to run based on what table name the user requests records from (how to know??)
                    # whatever happens here will return a processed gdf with records matching those existing in the table
                
                # load final geodataframe into postgres (TODO: replace 'gdf' with the processed gdf once funcs are in place)
                if data["objectid"] == ["all"] or 'all':
                    print('replacing all records')

                    # if the user is looking to refresh all records, replace the table with the new gdf...
                    engine = ef.mk_postgis_engine(dbase)
                    gdf.to_postgis(dbase['table'], engine, dbase['schema'], if_exists=dbase['if_exists']) # this is a geopandas method
                    
                    print('all records successfully refreshed in postgres')

                else:
                    # ...otherwise, just update the records that need it
                    print(f'updating records where OBJECTID = {data["objectid"]}')

                    ef.update_with_gdf(dbase, dbase["table"], gdf)

                # pull records from db as list of dictionaries
                records = ef.retrieve_from_postgis(dbase, dbase['table'], data['objectid'])

                # return records and/or message
                return {
                    "message": f"success: refreshed table {dbase['table']} for OBJECTID values {data['objectid']}",
                    "count": len(records),
                    "records": records
                }

            else:
                return {"message": "error: haven't developed handling for fields other than OBJECTID"}
            
    elif request.method == 'GET':
        records = TaxlotModel.query.all()
        results = [
            {
                'OBJECTID': record.OBJECTID,
                'ZONE_CMPLT': record.ZONE_CMPLT,
                'ZONE_CLASS': record.ZONE_CLASS,
                'ZONE_SMRY': record.ZONE_SMRY,
                'Shape__Area': record.Shape__Area,
                'Shape__Length': record.Shape__Length,
                'timestamp': record.timestamp,

            } for record in records]

        return {"message": "success: all records retrieved", "count": len(results), "records": results}

@app.route('/<record_id>', methods=['POST', 'GET'])
def handle_taxlot(record_id): # for engaging one record at a time
    record = TaxlotModel.query.get_or_404(record_id) # if the record_id isn't in the db, it cannot have a webpage

    # NOTE: the POST request {"objectid" : "all"} doesn't work at this route
    if request.method == 'POST':
        if request.is_json:
            data = request.get_json()

            # if fieldname == objectid, query with the objectid-based function
            if "objectid" in data and len(data) == 1:

                # NOTE: requests using {"objectid" : "all"} aren't permitted outside of the base route
                if data["objectid"] in (["all"], "all"):
                    return {"message": "error: refreshing records with 'all' is only supported at the base route '/'"}
                
                # get a geodataframe with the records corresponding with requested objectids
                gdf = ef.oids_to_gdf(dsource, data["objectid"])

                # TODO: Make functions to run based on what table name the user requests records from (how to know??)
                    # whatever happens here will return a processed gdf with records matching those existing in the table
                
                # load final geodataframe into postgres (TODO: replace 'gdf' with the processed gdf once funcs are in place)
                ef.update_with_gdf(dbase, dbase["table"], gdf)

                # pull records from db as list of dictionaries
                records = ef.retrieve_from_postgis(dbase, dbase['table'], data['objectid'])

                # return records and/or message
                return {
                    "message": f"success: refreshed table {dbase['table']} for OBJECTID values {data['objectid']}",
                    "count": len(records),
                    "records": records
                }

            else:
                return {"message": "error: haven't developed handling for fields other than OBJECTID"}

    elif request.method == 'GET':
        result = {
            'OBJECTID': record.OBJECTID,
            'ZONE_CMPLT': record.ZONE_CMPLT,
            'ZONE_CLASS': record.ZONE_CLASS,
            'ZONE_SMRY': record.ZONE_SMRY,
            'Shape__Area': record.Shape__Area,
            'Shape__Length': record.Shape__Length,
            'timestamp': record.timestamp,
        }
        return{"message": f"success: retrieved record corresponding with OBJECTID {record_id}", "record": result}

if __name__ == '__main__':
    app.run(debug=True)