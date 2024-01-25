# Technical test
This pipeline takes in a file and performs a series of data quality checks. It then populates two tables given that the data quality checks pass and an error log table which has the log of all the rows that failed the tests.

Prerequisits:
A connection to postgres sql is required and a .env file is to be created with the following format along with the path to the fileS
db_host = ""
db_database = ""
db_user = ""
db_password = ""
FILE_PATH = ""

To run the file simply install psycopg2 and the libraries in the main.py file and then open a terminal window and run python main.py