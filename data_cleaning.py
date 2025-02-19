from data_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd
import re

class DataCleaning:
    
    #cleans the data handling NULL and removing invalid rows
    def clean_user_data(self):
        
        db_connect = DatabaseConnector()
        #created a dataextractor instance used it to extract the users table
        db_extract = DataExtractor()
        table = db_extract.read_rds_table(db_connect, 'legacy_user')

        table['country_code'] = table['country_code'].astype('category')
        table['country'] = table['country'].astype('category')
        table['country_code'].replace('GGB', 'GB', inplace=True)

        # drops rows filled with nulls and the wrong values
        country_codes = {'GB', 'US', 'DE'}
        inconsistent_categories = set(table['country_code']) - country_codes
        inconsistent_rows = table['country_code'].isin(inconsistent_categories)
        table = table[~inconsistent_rows]

        # uses to_datetime() method to correct date entries for D.O.B column and join_date column and changes the datatype to datetime64
        table['date_of_birth'] = pd.to_datetime(table['date_of_birth'], infer_datetime_format=True, errors='coerce')
        table['date_of_birth'] = table['date_of_birth'].astype('datetime64[ns]')
        # removes timestamp from column as only the date is required 
        table['date_of_birth'] = table['date_of_birth'].dt.date

        table['join_date'] = pd.to_datetime(table['join_date'], infer_datetime_format=True, errors='coerce')
        table['join_date'] = table['join_date'].astype('datetime64[ns]')
        # removes timestamp from column as only the date is required 
        table['join_date'] = table['join_date'].dt.date

        # uploads the cleaned user details to the local postgres database
        db_connect.upload_to_db(table, 'dim_users')
