# TEST PARTE DOS


#Create the postgresql instance 

import google.auth
from googleapiclient.discovery import build

# Authenticate and create a client for the Cloud SQL API
credentials, project = google.auth.default()
service = build('sqladmin', 'v1beta4', credentials=credentials)

# Define the instance configuration
instance = {
    'name': 'my-instance',
    'region': 'us-central1',
    'databaseVersion': 'POSTGRES_11',
    'settings': {
        'tier': 'db-n1-standard-1'
    }
}

# Create the instance
response = service.instances().insert(project=project, body=instance).execute()
print('Instance created:', response['name'])

# Load the data into the database

import google.auth
from googleapiclient.discovery import build
import psycopg2

# Authenticate and create a client for the Sheets API
credentials, project = google.auth.default()
service = build('sheets', 'v4', credentials=credentials)

# Specify the spreadsheet ID and range for each tab
tab1_spreadsheet_id = 'SPREADSHEET_ID'
tab1_range = 'Sheet1!A1:E'
tab2_spreadsheet_id = 'SPREADSHEET_ID'
tab2_range = 'Sheet2!A1:H'

# Retrieve the data from each tab
tab1_response = service.spreadsheets().values().get(
    spreadsheetId=tab1_spreadsheet_id, range=tab1_range).execute()
tab1_values = tab1_response['values']
tab2_response = service.spreadsheets().values().get(
    spreadsheetId=tab2_spreadsheet_id, range=tab2_range).execute()
tab2_values = tab2_response['values']

# Connect to the PostgreSQL instance
connection = psycopg2.connect(
    host='INSTANCE_IP_ADDRESS',
    user='USERNAME',
    password='PASSWORD',
    database='DATABASE_NAME'
)
cursor = connection.cursor()

# Insert the data from tab 1 into a table
table1_name = 'main_user_info'
columns = tab1_values[0]
values = tab1_values[1:]
cursor.execute('TRUNCATE TABLE {};'.format(table1_name))
for row in values:
    cursor.execute(
        'INSERT INTO {} ({}) VALUES ({});'.format(
            table1_name,
            ', '.join(columns),
            ', '.join(['%s'] * len(columns))
        ),
        row
    )

# Insert the data from tab 2 into a table
table2_name = 'user_extra_info'
columns = tab2_values[0]
values = tab2_values[1:]
cursor.execute('TRUNCATE TABLE {};'.format(table2_name))
for row in values:
    cursor.execute(
        'INSERT INTO {} ({}) VALUES ({});'.format(
            table2_name,
            ', '.join(columns),
            ', '.join(['%s'] * len(columns))
        ),
        row
    )

# Commit the changes and close the connection
connection.commit()
connection.close()

#Transforming tables

import psycopg2
import json

# Connect to the PostgreSQL instance
conn = psycopg2.connect(host="<hostname>", database="<database>", user="<username>", password="<password>")

# Create a cursor
cur = conn.cursor()

# Filter the table to exclude any NULL values in the JSON object column
cur.execute("SELECT * FROM user_extra_info WHERE location_change_city_ids IS NOT NULL")

# Fetch and print the resulting rows
rows = cur.fetchall()
for row in rows:
    json_data = json.loads(row[location_change_city_ids])
    print(json_data)


columns
cur.execute("SELECT * FROM user_extra_info WHERE location_change_city_ids IS NOT NULL AND vacancy_area_id > 2 AND employment_status = 0")

# Fetch and print the resulting rows
rows = cur.fetchall()
for row in rows:
    json_data = json.loads(row[location_change_city_ids])
    print(json_data)


# Expand the years_experience column and add separate columns for years and months
cur.execute("ALTER TABLE user_extra_info ADD COLUMN years INTEGER, ADD COLUMN months INTEGER")

# Update the new columns with the values from the years_experience column
cur.execute("UPDATE user_extra_info SET years = <json_column>->>'years', months = <json_column>->>'months'")

# Drop the years_experience column
cur.execute("ALTER TABLE <table> DROP COLUMN <json_column>")

cur.execute("UPDATE main_user_info SET last_name = initcap(last_name)")

cur.execute("UPDATE main_user_info SET load_date = to_char(load_date, 'YYYY/MM/DD')")

# Close the cursor and connection
cur.close()
conn.close()

# Final step

import google.auth
from google.cloud import bigquery

# Set up the BigQuery client
credentials, project_id = google.auth.default()
client = bigquery.Client(credentials=credentials, project=project_id)

# Set the name of the destination table
table_id = "a_un_paso_de_trabajar_en_hunty"

# Set up the SQL query
query = f"""
    SELECT main_user_info.*, user_extra_info.*
    FROM main_user_info
    INNER JOIN user_extra_info
    ON main_user_info.user_id = user_extra_info.user_id
"""

# Create the destination table in BigQuery
table = bigquery.Table(f"{project_id}.{table_id}")
table = client.create_table(table)

# Execute the query and save the result in the destination table
job_config = bigquery.QueryJobConfig(destination=table)
query_job = client.query(query, job_config=job_config)
query_job.result()
