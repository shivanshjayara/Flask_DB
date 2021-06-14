import mysql.connector as connection
import csv
# import logging as lg
import pandas as pd


class mysql_db_class:
    def __init__(self, database_name, table_name):
        self.database_name = database_name
        self.table_name = table_name

    def create_database(self):
        try:
            mydb = connection.connect(host='localhost', user='root',
                                      passwd='shivansh@180cc',
                                      auth_plugin='mysql_native_password')
        except Exception as e:
            print(e)
        else:
            cur = mydb.cursor()
            cur.execute(f'CREATE DATABASE {self.database_name}')
            mydb=connection.connect(host='localhost', user='root',database=self.database_name, password='shivansh@180cc', auth_plugin='mysql_native_password')
            cur=mydb.cursor()
            cur.execute(f'''CREATE TABLE {self.table_name} 
            (customer_id varchar(100) primary key, 
            store_id varchar(100), 
            first_name varchar(50), 
            last_name varchar(50), 
            email varchar(50), 
            address_id varchar(60))''')

        finally:
            mydb.close()


    def insert_single(self):
        mydb = connection.connect(host='localhost', user='root', database=self.database_name, password='shivansh@180cc', auth_plugin='mysql_native_password')
        cur = mydb.cursor()
        cur.execute(f"INSERT INTO {self.table_name} VALUES ('0','0','shivansh','jayara','s4shivansh29@gmail.com','10')")
        mydb.commit()
        mydb.close()

    def update_table(self):
        try:
            mydb = connection.connect(host='localhost', user='root', database=self.database_name, password='shivansh@180cc', auth_plugin='mysql_native_password')
            cur = mydb.cursor()
        except Exception as e:
            print(e)
        else:
            try:
                cur.execute(f"UPDATE {self.table_name} SET last_name='singh' WHERE last_name='jayara'")
            except Exception as e:
                print(e)

        finally:
            mydb.commit()
            mydb.close()

    def bulk_insert(self):
        try:
            mydb = connection.connect(host='localhost', user='root', database=self.database_name, password='shivansh@180cc', auth_plugin='mysql_native_password')
            cur=mydb.cursor()
            data = pd.read_csv('customer_db.csv')
            data.columns = ['customer_id', 'store_id', 'first_name', 'last_name', 'email', 'address_id']
            for row in data.itertuples():
                sql = f"INSERT INTO {self.table_name} (customer_id, store_id, first_name, last_name, email, address_id) VALUES (%s,%s,%s,%s,%s,%s)"
                val = (row.customer_id, row.store_id, row.first_name, row.last_name, row.email, row.address_id)
                cur.execute(sql, val)
                mydb.commit()

        except Exception as e:
            print(e)
        finally:
            mydb.close()

    def delete_row(self):
        mydb = connection.connect(host='localhost', user='root', database=self.database_name, password='shivansh@180cc', auth_plugin='mysql_native_password')
        cur=mydb.cursor()
        cur.execute(f'DELETE FROM {self.table_name} where first_name="MARY"')
        mydb.commit()
        mydb.close()

    def download(self):
        mydb = connection.connect(host='localhost', user='root', database=self.database_name, password='shivansh@180cc', auth_plugin='mysql_native_password')
        cur=mydb.cursor()
        df= pd.read_sql(f'select * from {self.table_name}',mydb)
        df.to_csv('mysql_updated_customer_db.csv')
        mydb.close()

