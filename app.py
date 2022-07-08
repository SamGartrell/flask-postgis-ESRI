#!/bin/sh
"exec" "`dirname $0`/python" "$0" "$@"

from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy as sa
from flask_migrate import Migrate as mi

# Entering the flask migrate commands 'flask db migrate' and 'flask db upgrade'
# in the command line (while flask app is running) will pass any column changes
# to the database

# postgres variables
usr = 'postgres'
pw = 'Only3Follicles'
host = 'localhost'
port = '' # no port bc localhost
dbase = 'api_test' # not to be confused with variable 'db', a flask-sa instance

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{usr}:{pw}@{host}/{dbase}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = sa(app)
migrate = mi(app, db)

class CheeseModel(db.Model):
    __tablename__ = 'cheese'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable = False)
    category = db.Column(db.String(100), nullable = False)
    notes = db.Column(db.String(100), nullable = True)

    def __init__(self, name, category, notes):
        self.name = name
        self.category = category
        self.notes = notes

    def __repr__(self):
        return f'<Cheese {self.name}>'


@app.route('/')
def redirect():
    return 'add "/cheese" to the url'

@app.route('/cheese', methods=['POST', 'GET'])
def handle_cheeses():  #for record creation and getting ALL records
    if request.method == 'POST':
        if request.is_json:
            data = request.get_json()
            new_cheese = CheeseModel(
                name=data['name'],
                category=data['category'],
                notes=data['notes']
                )

            db.session.add(new_cheese)
            db.session.commit()  # closes the db transaction and saves input

            return {"message": f"cheese {new_cheese.name} created successfully"}
        else:
            return {"error'" "the request payload is not in JSON format"}

    elif request.method == 'GET':
        cheeses = CheeseModel.query.all()
        results = [
            {
                'name': cheese.name,
                'category': cheese.category,
                'notes': cheese.notes
            } for cheese in cheeses]

        return {"count": len(results), "cheeses": results}

@app.route('/cheese/<cheese_id>', methods=['GET', 'PUT', 'DELETE'])
def handle_cheese(cheese_id): # for engaging one record at a time
    cheese = CheeseModel.query.get_or_404(cheese_id) # if the cheese_id isn't in the db, it cannot have a webpage

    if request.method == 'GET':
        result = {
            'name': cheese.name,
            'category': cheese.category,
            'notes': cheese.notes
        }
        return{"message": "success", "cheese": result}
    
    elif request.method == 'PUT':
        data = request.get_json()
        cheese.name = data['name']
        cheese.category = data['category']
        cheese.notes = data['notes']
        db.session.add(cheese)
        db.session.commit()
        return {"message": f"cheese {cheese.name} successfully updated"}

    elif request.method == 'DELETE':
        db.session.delete(cheese)
        db.session.commit()
        return {"message": f"Cheese {cheese.name} successfully deleted."}


if __name__ == '__main__':
    app.run(debug=True)