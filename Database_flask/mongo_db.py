import pymongo
from csv_2_dict import csv_dict
import pandas as pd

client = pymongo.MongoClient('mongodb://localhost:27017/')

class mongo_db_class:
    def __init__(self, database_name, collection_name):
        self.database = database_name
        self.collection = collection_name

    try:
        def create(self):
            mongodb = client['mongo_flask_db']
            collection = mongodb[self.collection]
            return collection
    except Exception as e:
        print(e)

    try:
        def insert_single_document(self):
            document_single = {'customer_id': 0,
                               'store_id': 0,
                               'first_name': 'shivansh',
                               'last_name': 'jayara',
                               'email': 'shivansh@gmail.com',
                               'address': 10,
                               'create_date': '01-06-2021'
                               }
            self.create().insert_one(document_single)
    except Exception as e:
        print(e)

    try:
        def update_document(self):
            parent_data = {'last_name': 'jayara'}
            updated_data = {'$set': {'last_name': 'Singh'}}
            self.create().update_one(parent_data,updated_data)
    except Exception as e:
        print(e)

    try:
        def insert_many_document(self):
            document_many = csv_dict()
            convert = document_many.convert_to_dict()
            self.create().insert_many(convert)
    except Exception as e:
        print(e)

    try:
        def delete_document(self):
            delete_data = {'customer_id': {'$in': ['3', '5', '6']}}
            self.create().delete_many(delete_data)
    except Exception as e:
        print(e)

    try:
        def download(self):
            all_data = self.create().find({})
            df = pd.DataFrame(all_data)
            df.to_csv('mongodb_updated_customer_db.csv', index=False)
    except Exception as e:
        print(e)
    finally:
        client.close()
