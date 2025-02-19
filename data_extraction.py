from data_utils import DatabaseConnector
import pandas as pd
from sqlalchemy import create_engine, text, inspect
import tabula




class DataExtractor:

    def __init__ (self, database_connector):
        self.database_connector = database_connector

    #This method reads a table from the RDS database and returns it as a pandas DataFrame. 
    def read_rds_table(self, table):
        engine = self.database_connector.init_db_engine()
        
        query = (f"SELECT * FROM {table}")
        table_df= pd.read_sql_query(sql=text(query), con=engine.connect())
        return table_df
    
    def retrieve_pds_data(self, link):

        table = tabula.read_pdf(link, pages='all')
        table_df = pd.concat(table)

        return table_df

