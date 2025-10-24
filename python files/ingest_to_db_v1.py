import mysql.connector
import pandas as pd
from simple_salesforce import Salesforce
from datetime import datetime,timedelta
import sys
import logging


yesterday = datetime.today().date() - timedelta(days=1)

def get_salesforce_connection():
    SALESFORCE_USERNAME = 'it@woodfordbros.com'
    SALESFORCE_PASSWORD = 'Scott667'
    SALESFORCE_SECURITY_TOKEN = 'NwnMCGMzcpT6BiegYrbEYVaOg'
    sf = Salesforce(username=SALESFORCE_USERNAME, password=SALESFORCE_PASSWORD, security_token=SALESFORCE_SECURITY_TOKEN)
    return sf



def get_database_connection():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            password='Nouman000',
            database='pre_stage'
        )
        cursor = connection.cursor()
        print('Database connection successful')
        return cursor, connection
    except mysql.connector.Error as error:
        print("Error connecting to MySQL database:", error)
        logging.error("Error connecting to MySQL database:", error)
        return None, None
    

tables_name= {
    'i360__Appointment__c': 'appointment',
    'i360__Marketing_Source__c': 'marketing_source',
    'i360__Lead_Source__c': 'lead_source',
    'i360__Prospect__c': 'prospect',
    'i360__Sale__c': 'sale',
    'i360__Source_Cost__c': 'source_cost',
    'i360__Project__c': 'project'
}


def map_dtype_to_mysql(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    else:  # Default to string-like
        return "TEXT"
    

def insert_into_db(leads_df,sql_name):
    try:
        # Replace NaN with None explicitly for compatibility with MySQL
        leads_df = leads_df.where(pd.notnull(leads_df), None)

        # Get database connection and cursor
        cursor, connection = get_database_connection()
        try:
            cursor.execute(f"""TRUNCATE TABLE {sql_name}""")
            connection.commit()
            print("table trunctad")
        except Exception as e:
            print(f"Error occurred: {e}")
        cursor.execute("SET GLOBAL  max_allowed_packet =  1024 * 1024 * 1024 * 1024;")  # 64MB
        print("Adjusted max_allowed_packet to 64MB for the current session.")
        # cursor.execute("SET GLOBAL  max_allowed_packet = 67108864;")  # 64MB
        # print("Adjusted max_allowed_packet to 64MB for the current session.")
        # Prepare the insertion query
        insert_query = f"""
        INSERT IGNORE INTO pre_stage.{sql_name} ({', '.join(f'`{col}`' for col in leads_df.columns)}) 
        VALUES ({', '.join(['%s' for _ in leads_df.columns])});
        """
        
        # Convert DataFrame rows to tuples for insertion, ensuring None is used for null values
        data_to_insert = [tuple(row) if all(pd.notnull(row)) else tuple(None if pd.isnull(cell) else cell for cell in row) for row in leads_df.to_numpy()]
        batch_size = 5000

        # Insert in batches of 1000 rows
        for i in range(0, len(data_to_insert), batch_size):
            batch = data_to_insert[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()
            print(f"Inserted batch {i // batch_size + 1}")
           

        print(f"{len(data_to_insert)} rows inserted successfully into {sql_name}.")
        logging.info(f"{len(data_to_insert)} rows inserted successfully into {sql_name}.")


    except Exception as e:
        print(f"Error occurred: {e}")
        logging.error(f"Error occurred: {e}")
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("Database connection closed.")





def  ingestion():
    logging.basicConfig(filename="script_log.txt", level=logging.INFO, format="%(asctime)s - %(message)s", filemode="w")
    sys.stdout = open("script_output.txt", "w")
    sys.stderr = open("script_error.txt", "w")
    try:

        sf = get_salesforce_connection()

        for key,value in tables_name.items():
            print(f"Describing object: {key}")
            logging.info(f"Describing object: {key}")

            object_to_describe = getattr(sf, key)  # Use dynamic API call
            object_description = object_to_describe.describe()
            fields = [field['name'] for field in object_description['fields']]
            field_names = ", ".join(fields)

            # query = f"""
            # SELECT {field_names}
            # FROM  {key}  WHERE DAY_ONLY(CreatedDate) >= {yesterday}
            # """
            query = f"""
            SELECT {field_names}
            FROM  {key}  
            """
            leads = sf.query_all(query)
            leads_data = [lead for lead in leads['records']]  
            for lead in leads_data:
                lead.pop('attributes', None)
            leads_df = pd.DataFrame(leads_data)
            insert_into_db(leads_df,value)

    except Exception as e:
        print(f"Error occurred: {e}")
        logging.error(f"Error occurred: {e}")
    logging.info("===== SCRIPT EXECUTION COMPLETED =====")




if __name__ == "__main__":
        ingestion()
   