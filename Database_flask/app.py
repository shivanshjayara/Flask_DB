from flask import Flask, request, jsonify
from mongo_db import mongo_db_class
from mysql_db import mysql_db_class
import logging as lg

app = Flask(__name__)
lg.basicConfig(filename='flask_log_file.txt', level=lg.INFO,
               format='%(asctime)s, %(name)s, %(levelname)s, %(message)s', datefmt='%Y-%m-%d')


@app.route('/create', methods=['POST'])
def create_database():
    db_name = 'flask1_database'
    table_name = 'customer_details'
    lg.info('Checking for method type')
    try:
        lg.info('Checking for Method whether it is POST or GET')
        if request.method == 'POST':
            lg.info('Method was found "POST"')

            database_type = request.json['Database_Type']
            lg.info(
                'Setting database type variable for entering the type of database which you want to proceed with')

            lg.info('Checking which type a user has entered: Mysql or Mongodb Or Cassandra')
            if database_type == 'mysql':
                lg.info('User has mention MySql database')
                try:
                    lg.info('Creating an object of "class mysql_db_class"')
                    mysql_object = mysql_db_class(db_name, table_name)

                    lg.info(f'Calling "create database function" which will create a database: {db_name}')
                    mysql_object.create_database()

                except Exception as e:
                    print(e)
                    lg.error(e)

                lg.info('Database and table both has been created. For confirmation please check in your workbench')
                return jsonify(
                    f'{database_type} database and table are created. Database name: {db_name} and table name :{table_name}')

            if database_type == 'mongodb':
                lg.info('User has mention MongoDB database')

                lg.info('Creating an object of "mongo_db_class"')
                mongodb_object = mongo_db_class(db_name, table_name)

                lg.info(f'Calling "create function" which will create database: {db_name} and table: {table_name}')
                mongodb_object.create()

                lg.info('Finally database and table both are created now')
                return jsonify(
                    f'{database_type} database and collection both are created. Database name is: {db_name} and collection name is: {table_name}')
    except Exception as e:
        print(e)
        lg.error(e)


@app.route('/insert_single', methods=['POST'])
def insert():
    db_name = 'flask1_database'
    table_name = 'customer_details'
    lg.info(f'Calling insertion function just for inserting a single record into table {table_name}')

    lg.info('Checking for Method whether it is POST or GET')
    if request.method == 'POST':
        lg.info('Method was found "POST"')

        lg.info('Checking which type a user has entered: Mysql or Mongodb Or Cassandra')
        lg.info('Declaring a variable which will receive a database type from the user')
        database_type = request.json['Database_Type']
        if database_type == 'mysql':
            lg.info('User has mention MySql database')

            lg.info('Creating an object of "mysql_db_class"')
            mysql_object = mysql_db_class(db_name, table_name)

            lg.info(
                f'Calling "insert single record function" which will insert a single record in table: {table_name}')
            mysql_object.insert_single()

            lg.info('Finally single record is inserted')
            return jsonify(
                f'Single Data is inserted successfully in table: {table_name} in database: {db_name} of {database_type}')

        if database_type == 'mongodb':
            lg.info('User has mention MongoDB database')

            lg.info('Creating an object of "mongo_db_class"')
            mongodb_object = mongo_db_class(db_name, table_name)

            lg.info(
                f'Calling "insert_single_document function" which will insert a single record in table: {table_name}')
            mongodb_object.insert_single_document()
            lg.info('Finally single record is inserted')
            return jsonify(
                f'Single Data is inserted successfully in table: {table_name} in database: {db_name} of {database_type}')


@app.route('/update_table', methods=['POST'])
def update_table():
    db_name = 'flask1_database'
    table_name = 'customer_details'
    lg.info(f'Calling update function for updating table {table_name}')

    lg.info('Checking for Method whether it is POST or GET')
    if request.method == 'POST':
        lg.info('Method was found "POST"')

        lg.info('Checking which type a user has entered: Mysql or Mongodb Or Cassandra')
        lg.info('Declaring a variable which will receive a database type from the user')
        database_type = request.json['Database_Type']

        if database_type == 'mysql':
            lg.info('User has mention MySql database')

            lg.info('Creating an object of "mysql_db_class"')
            mysql_object = mysql_db_class(db_name, table_name)

            lg.info(f'Calling "update table function" which will insert a single record in table: {table_name}')
            mysql_object.update_table()
            lg.info('Finally table is updated')
            return jsonify(
                f'Data is updated successfully in table: {table_name} in database: {db_name} of {database_type}')

        if database_type == 'mongodb':
            lg.info('User has mention Mongodb database')

            lg.info('Creating an object of "mongodb_db_class"')
            mongodb_object = mongo_db_class(db_name, table_name)

            lg.info(f'Calling "update_document function" which will insert a single record in table: {table_name}')
            mongodb_object.update_document()
            lg.info('Finally document is updated')
            return jsonify(
                f'Data is updated successfully in table: {table_name} in database: {db_name} of {database_type}')


@app.route('/multiple_insert', methods=['POST'])
def multiple_insert():
    db_name = 'flask1_database'
    table_name = 'customer_details'
    lg.info(f'Calling multiple insertion function just for inserting a single record into table {table_name}')

    lg.info('Checking for Method whether it is POST or GET')
    if request.method == 'POST':
        lg.info('Method was found "POST"')

        lg.info('Checking which type a user has entered: Mysql or Mongodb Or Cassandra')
        lg.info('Declaring a variable which will receive a database type from the user')
        database_type = request.json['Database_Type']

        if database_type == 'mysql':
            lg.info('User has mention MySql database')

            lg.info('Creating an object of "mysql_db_class"')
            mysql_object = mysql_db_class(db_name, table_name)

            lg.info(f'Calling "Bulk_insert function" which will insert a single record in table: {table_name}')
            mysql_object.bulk_insert()
            lg.info('Finally table is updated')
            return jsonify(
                f'Bulk Data is inserted successfully in table: {table_name} in database: {db_name} of {database_type}')

        if database_type == 'mongodb':
            lg.info('User has mention Mongodb database')

            lg.info('Creating an object of "mongodb_db_class"')
            mongodb_object = mongo_db_class(db_name, table_name)

            lg.info(
                f'Calling "insert_many_document function" which will insert a single record in table: {table_name}')
            mongodb_object.insert_many_document()

            lg.info('Finally bulk records are inserted in document')
            return jsonify(
                f'Bulk Data is inserted successfully in table: {table_name} in database: {db_name} of {database_type}')


@app.route('/delete_data', methods=['POST'])
def delete():
    db_name = 'flask1_database'
    table_name = 'customer_details'
    lg.info(f'Calling delete function for deleting record into table {table_name}')

    lg.info('Checking for Method whether it is POST or GET')
    if request.method == 'POST':
        lg.info('Method was found "POST"')

        lg.info('Checking which type a user has entered: Mysql or Mongodb Or Cassandra')
        lg.info('Declaring a variable which will receive a database type from the user')
        database_type = request.json['Database_Type']

        if database_type == 'mysql':
            lg.info('User has mention MySql database')

            lg.info('Creating an object of "mysql_db_class"')
            mysql_object = mysql_db_class(db_name, table_name)

            lg.info(f'Calling "delete_row function" which will insert a single record in table: {table_name}')
            mysql_object.delete_row()

            lg.info('Finally record is deleted')
            return jsonify(
                f'Data is deleted successfully in table: {table_name} in database: {db_name} of {database_type}')

        if database_type == 'mongodb':
            lg.info('User has mention Mongodb database')

            lg.info('Creating an object of "mongodb_db_class"')
            mongodb_object = mongo_db_class(db_name, table_name)

            lg.info(
                f'Calling "insert_many_document function" which will insert a single record in table: {table_name}')
            mongodb_object.delete_document()

            lg.info('Finally document is deleted')
            return jsonify(
                f'Data is deleted successfully in table: {table_name} in database: {db_name} of {database_type}')


@app.route('/download_data', methods=['POST'])
def download():
    lg.info('Downloading of a file will be done')
    db_name = 'flask1_database'
    table_name = 'customer_details'

    if request.method == 'POST':
        database_type = request.json['Database_Type']

        if database_type == 'mysql':
            lg.info('File is downloading from mysql database table into some csv file')
            mysql_object = mysql_db_class(db_name, table_name)
            mysql_object.download()
            return jsonify(
                f'Data has been downloaded to csv file from table: {table_name} in database: {db_name} of {database_type}')

        if database_type == 'mongodb':
            lg.info('File is downloading from mongodb database table into some csv file')

            mongodb_object = mongo_db_class(db_name, table_name)
            mongodb_object.download()
            return jsonify(
                f'Data has been downloaded to csv file from table: {table_name} in database: {db_name} of {database_type}')

if __name__ == '__main__':
    app.run(debug=True)
lg.shutdown()