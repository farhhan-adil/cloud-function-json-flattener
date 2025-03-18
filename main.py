import re
import json
import os
import time
import pandas as pd
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime

# Set up the clients
try:
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()
    print("Successfully initialized Cloud Storage and BigQuery clients.")
except Exception as e:
    print(f"Error initializing clients: {str(e)}")

# Environment variables
dataset_name = os.getenv('DATASET_NAME')        # BigQuery dataset where the table will be created


def sanitize_column_name(name):
    """
    Sanitize column names by:
    - Replacing '%' with 'percent'.
    - Replacing '/' with '_per_
    - Replacing any other special character or space with '_'.
    - Replacing consecutive special characters with a single '_'.
    """
    name = name.replace('%', 'percent')         # Replace '%' with 'percent'
    name = name.replace('/', '_per_')           # Replace '/' with '_per_'
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)  # Replace all non-alphanumeric characters
    name = re.sub(r'_+', '_', name)             # Replace consecutive '_' with a single '_'
    name = name.strip('_')                      # Remove leading or trailing '_'

    return name

def flatten_json(nested_json, parent_key='', sep='_'):
    """
    Recursively flatten a nested JSON object, including lists, and sanitize keys.
    """
    flat_dict = {}
    try:
        # Iterate over all keys in the JSON
        for key, value in nested_json.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            
            # If the value is a dict, recurse
            if isinstance(value, dict):
                flat_dict.update(flatten_json(value, new_key, sep=sep))
            
            # If the value is a list, handle each element
            elif isinstance(value, list):
                # Check if it's a list of dictionaries (common in nested JSON)
                if all(isinstance(item, dict) for item in value):
                    for i, item in enumerate(value):
                        flat_dict.update(flatten_json(item, f"{new_key}{sep}{i}", sep=sep))
                else:
                    flat_dict[new_key] = ', '.join(map(str, value))  # Join list items as strings
            
            # Otherwise, it's a regular value
            else:
                flat_dict[new_key] = value
        
    except Exception as e:
        print(f"Error flattening JSON: {str(e)}")
    return flat_dict

def handle_case_insensitive_duplicates(df):
    """
    Merge columns that differ only by case sensitivity.
    Keep the first occurrence and drop others after merging their data.
    """
    lowercase_cols = {}
    columns_to_drop = []
    
    for col in df.columns:
        lower_col = col.lower()
        if lower_col in lowercase_cols:
            # Column already exists, merge the data
            existing_col = lowercase_cols[lower_col]
            # Merge data: fill missing values in the existing column with values from the current column
            df[existing_col] = df[existing_col].combine_first(df[col])
            # Mark the current column for dropping
            columns_to_drop.append(col)
        else:
            # First time encountering this column (case-insensitive)
            lowercase_cols[lower_col] = col
    
    # Drop the duplicate columns while keeping the merged data
    df = df.drop(columns=columns_to_drop)
    
    return df

def schema_enforcement(df, table_id):
    """
    Compare the schema of the existing table with the df.
    Handling schema type mismatch by updating data types of pandas df.
    """

    # Check if the table exists
    try:
        # Try fetching the table schema to check if the table exists
        table = bigquery_client.get_table(table_id)
        table_exists = True
    except NotFound:
        table_exists = False

    try:
        # If the table exists, ensure the DataFrame's schema matches the table schema
        if table_exists:

            print("Table already exist. Handling schema type mismatch.")
            # Get the schema from the existing table
            existing_schema = {field.name: field.field_type for field in table.schema}

            # Data type mapping between Pandas and BigQuery types
            type_mapping = {
                'STRING': object,
                'INTEGER': 'int64',
                'FLOAT': float,
                'BOOLEAN': bool,
                'DATE': 'datetime64[ns]',
                'DATETIME': 'datetime64[ns]',
                'TIMESTAMP': 'datetime64[ns]',
                'NUMERIC': float,
                'BIGNUMERIC': float
            }

            # Ensure the data types in the DataFrame match the BigQuery schema
            for column, dtype in df.dtypes.items():
                if column in existing_schema:
                    bq_type = existing_schema[column]
                    # Get the pandas type equivalent for BigQuery type
                    pandas_type = type_mapping.get(bq_type, None)
                    
                    if pandas_type:
                        # Cast pandas df datatype to match with already existing table
                        if pandas_type == 'int64':
                            pandas_type = 'Int64'       # Use pandas' nullable integer type
                        elif pandas_type == object:     
                            pandas_type = str           # Use pandas' string type
                        #print(f"Upgrading column '{column}' from {dtype} to {pandas_type}")
                        df[column] = df[column].astype(pandas_type)
                    else:
                        print(f"No mapping for BigQuery type '{bq_type}' for column '{column}'")

    except Exception as e:
        print(f"Error upgrading datatype : {str(e)}")
    return df

def process_json_file(event, context):
    """
    Cloud Function triggered when a new file is uploaded to the Cloud Storage bucket.
    """
    try:
        file_name = event['name']
        bucket_name = event['bucket']  

        print(f"Processing file: {file_name} from bucket: {bucket_name}")

        # Fetch bucket and blob
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Download the JSON file
        print(f"Downloading file {file_name} from bucket {bucket_name}")
        json_data = json.loads(blob.download_as_string())
        print(f"File {file_name} downloaded successfully.")

        # Flatten the JSON data
        print(f"Flattening JSON data for file {file_name}")
        if isinstance(json_data, dict):                                                     # If the top level is a dictionary and contains a list inside
            flattened_data = [flatten_json(json_data)]                                      # Flatten the dictionary
            for key, value in json_data.items():                                            # Check for any lists inside the dictionary and process them
                if isinstance(value, list) and all(isinstance(item, dict) for item in value):
                    flattened_data = [flatten_json(item) for item in value]                 # If it's a list of dictionaries, flatten each item in the list
        elif isinstance(json_data, list):                                                   # If the top level is a list
            flattened_data = [flatten_json(item) for item in json_data]
        print(f"JSON data flattened successfully.")

        if flattened_data == []:
            print(f"Empty source file: {file_name}.")
            return f"Empty source file: {file_name}.", 200

        # Create a pandas DataFrame from the flattened data
        df = pd.DataFrame(flattened_data)

        # Sanitize column names all at once using pandas
        df.columns = [sanitize_column_name(col) for col in df.columns]

        # Handle duplicate columns due to case sensitivity
        df = handle_case_insensitive_duplicates(df)

        # Define BigQuery table ID dynamically (in the format `project_id.dataset_id.table_id`)
        source_file_name = os.path.basename(file_name)
        base_name = source_file_name.split('.')[0]  # Remove extension
        table_name =  '_'.join(base_name.split('_')[:-2])  # Remove the timestamp part
        table_id = f"{bigquery_client.project}.{dataset_name}.{table_name}"

        #Handling schema type mismatch 
        df = schema_enforcement(df, table_id)

        # To avoid fragmentation issues, creating a copy of the DataFrame
        df = df.copy()

        # Add 'create_date' and 'source_file_name' columns
        df['create_date'] = datetime.utcnow()
        df['source_file_name'] = source_file_name

        # Insert data directly into BigQuery using pandas' to_gbq()
        print(f"Inserting data from file {file_name} into BigQuery table {table_id}")
        df.to_gbq(destination_table=table_id, project_id=bigquery_client.project, if_exists='append')

        print(f"Data successfully inserted into {table_id}.")

    except Exception as e:
        print(f"Error processing JSON file: {str(e)}")