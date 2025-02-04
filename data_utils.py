import yaml
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd

class DatabaseConnector:

        # opens yaml file containing db credentials and saves them in a python dict, returns the dict
    def read_db_creds(self, creds_file ="db_creds.yaml"):

        with open(creds_file, 'r') as db_creds:
            db_dict = yaml.safe_load(db_creds)

        return db_dict
    
    #this method intialises a sqalchemy engine using the dv_creds and returns the engine
    def init_db_engine(self):
        db_creds = self.read_db_creds('db_creds.yaml')
        engine = create_engine(f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")

        return engine
    
    def list_db_tables(self):
         engine = self.init_db_engine()
         inspector = inspect(engine) #sqalchemy inspector object
         tables = inspector.get_table_names()

         print(tables)


         


