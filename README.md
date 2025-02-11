# Multinational Retail Data Centralisation

In this project I'm acting as an employee of a multinational retail company that sells products accross the world.

Currently, their sales data is spread across many different data sources so its quire difficult to analyse or access by memebers of the team 

In an effort to become more data-driven the organisation would like to make its sales data accessible from one centralised location.

My first goal will be to produce a system that stores the current company data in a database so it's accessed from one centralised location and acts as a single source of trust for sales data. 

I will then query the database to get up-to-date metrics for business. 

## Milestone 1 

- Creates a remote github repository for this project to version control the software.

- Connects the remote repository to a local clone using the command line.

```bash
git clone https://github.com/talhahaziz/multinational-retail-data-centralisation
```

## Milestone 2

- Set up a database 'sales_data' within pgadmin4. I will use this database to store company information that I extract from various sources. 

- Initialised the three classes which will be used to clean, extract and connect the data

    1. DataExtractor class - responsible for methods used to extract data from the data sources. The sources will include a  CSV file, an API and an S3 bucket.

    1. DatabaseConnector class - used to connect to the database and upload the cleaned data ready for analysis.

    1. DataCleaning class - include methods to clean the data from the various sources. 

- Added the database credentials to the .yaml file and included a .gitignore to ensure the credentials are not uploaded to Github.

- Created the following methods in the DatabaseConnector class which will be used to connect and retrieve data from the AWS database:

    1. read_db_creds(): read the AWS database credentials from a YAML file and return a python dictionary of said credentials. 

    1. init_db_engine(): uses the read_db_cred() method to retrieve database credentials and uses these to initialise and return a SQLalchemy engine connecting to the database.

    1. list_db_tables(): lists the tables present in the database so I know which tables I can extract the data from

    1. upload_to_db(): takes in a pandas dataframe and table name and uploads them to the local sales_db database setup within pgadmin4
