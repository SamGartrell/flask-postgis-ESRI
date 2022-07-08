"""
home of the flask API
"""

from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy as sa
from flask_migrate import Migrate as mi

# postgres variables
usr = 'postgres'
pw = 'Only3Follicles'
host = 'localhost'
port = '' # no port bc localhost
dbase = 'bwa_1' # not to be confused with variable 'db', a flask-sa instance

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{usr}:{pw}@{host}/{dbase}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = sa(app)
migrate = mi(app, db)

class TaxlotModel(db.Model):
    __tablename__ = 'taxlots'

    OBJECTID = db.Column(db.BigInteger, primary_key=True)
    ZONE_CMPLT = db.Column(db.Text)
    ZONE_CLASS = db.Column(db.Text)
    ZONE_SMRY = db.Column(db.Text)
    Shape__Area = db.Column(db.Float)  # change to decimal.decimal?
    Shape__Length = db.Column(db.Float) # ref: https://docs.sqlalchemy.org/en/14/core/type_basics.html#sqlalchemy.types.Float
    timestamp = db.Column(db.Float)
    # geometry = db.Column(db.Text) # leaving this out bc we don't need it right?

    # TODO: remove caps from fieldnames

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
    if request.method == 'POST':
        if request.is_json:
            data = request.get_json()
            new_record = TaxlotModel(
                objectid=data['OBJECTID'],
                zone_cmplt=data['ZONE_CMPLT'],  # check capitalization alternatives for debugging
                zone_class=data['ZONE_CLASS'],
                zone_smry=data['ZONE_SMRY'],
                shape_area=data['Shape__Area'],
                shape_length=data['Shape__Length'],
                timestamp=data['timestamp']
                )

            db.session.add(new_record)
            db.session.commit()  # closes the db transaction and saves input

            return {"message": f"taxlot #{new_record.objectid} created successfully"}
        else:
            return {"error": "the request payload is not in JSON format"}

    elif request.method == 'GET':
        records = TaxlotModel.query.all()
        results = [
            {
                'objectid': record.OBJECTID,
                'ZONE_CMPLT': record.ZONE_CMPLT,
                'ZONE_CLASS': record.ZONE_CLASS,
                'ZONE_SMRY': record.ZONE_SMRY,
                'Shape__Area': record.Shape__Area,
                'Shape__Length': record.Shape__Length,
                'timestamp': record.timestamp,

            } for record in records]

        return {"count": len(results), "records": results}

@app.route('/<record_id>', methods=['GET', 'PUT', 'DELETE'])
def handle_taxlot(record_id): # for engaging one record at a time
    record = TaxlotModel.query.get_or_404(record_id) # if the record_id isn't in the db, it cannot have a webpage

    if request.method == 'GET':
        result = {
            'objectid': record.OBJECTID,
            'ZONE_CMPLT': record.ZONE_CMPLT,
            'ZONE_CLASS': record.ZONE_CLASS,
            'ZONE_SMRY': record.ZONE_SMRY,
            'Shape__Area': record.Shape__Area,
            'Shape__Length': record.Shape__Length,
            'timestamp': record.timestamp,
        }
        return{"message": "success", "record": result}
    
    elif request.method == 'PUT':
        data = request.get_json()

        record.objectid=data['OBJECTID'],
        record.zone_cmplt=data['ZONE_CMPLT'],  # check capitalization alternatives for debugging
        record.zone_class=data['ZONE_CLASS'],
        record.zone_smry=data['ZONE_SMRY'],
        record.shape_area=data['Shape__Area'],
        record.shape_length=data['Shape__Length'],
        record.timestamp=data['timestamp']

        db.session.add(record)
        db.session.commit()
        return {"message": f"taxlot #{record.objectid} successfully updated"}

    elif request.method == 'DELETE':
        db.session.delete(record)
        db.session.commit()
        return {"message": f"taxlot #{record.objectid} successfully deleted"}


if __name__ == '__main__':
    app.run(debug=True)