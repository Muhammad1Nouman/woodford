import mysql.connector
import pandas as pd
from simple_salesforce import Salesforce
from datetime import datetime,timedelta


# yesterday = datetime.today().date()-timedelta(days=1)

def get_salesforce_connection():
    SALESFORCE_USERNAME = 'it@woodfordbros.com'
    SALESFORCE_PASSWORD = 'Woodford8971@!'
    SALESFORCE_SECURITY_TOKEN = '3S9gIhKeh2ZR7yYJrgFZBxLjV'
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
        return None, None
    

tables_name= {
    'i360__Appointment__c': 'appointment',
    'i360__Marketing_Source__c': 'marketing_source',
    'i360__Lead_Source__c': 'lead_source',
    'i360__Project__c': 'project',
    'i360__Prospect__c': 'prospect',
    'i360__Sale__c': 'sale', 
    'i360__Source_Cost__c': 'source_cost',
    'i360__Staff__c':'staff',
    'Project_Week_Snapshot__c':'project_week_snapshot'
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
    

def drop_and_create_table(sql_name, leads_df):
    """
    Drops the table if it exists and creates a new table based on the DataFrame's schema.
    """
    try:
        # Get database connection and cursor
        cursor, connection = get_database_connection()

        # Drop the table if it exists
        drop_query = f"DROP TABLE IF EXISTS pre_stage.{sql_name}_temp;"
        cursor.execute(drop_query)
        # print(f"Table {sql_name} dropped successfully.")

        # Generate the CREATE TABLE query dynamically
        create_columns = []
        for col in leads_df.columns:
            col_type = map_dtype_to_mysql(leads_df[col].dtype)
            create_columns.append(f"`{col}` {col_type}")

        create_query = f"""
        CREATE TABLE pre_stage.{sql_name}_temp (
            {', '.join(create_columns)}
        );
        """
        cursor.execute(create_query)
        print(f"Table {sql_name} created successfully with schema: {', '.join(create_columns)}")

        # Commit changes
        connection.commit()

    except Exception as e:
        print(f"Error while dropping/creating table: {e}")
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("Database connection closed.")

def insert_into_db(leads_df,sql_name):
    try:
        # Replace NaN with None explicitly for compatibility with MySQL
        leads_df = leads_df.where(pd.notnull(leads_df), None)

        # Get database connection and cursor
        cursor, connection = get_database_connection()
        cursor.execute("SET GLOBAL  max_allowed_packet =  1024 * 1024 * 1024 * 1024;")
        # Prepare the insertion query
        insert_query = f"""
        INSERT INTO pre_stage.{sql_name}_temp ({', '.join(f'`{col}`' for col in leads_df.columns)}) 
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

        print(f"{len(data_to_insert)} rows inserted successfully into {sql_name}_temp.")

        drop_query = f"DROP TABLE IF EXISTS pre_stage.{sql_name};"
        cursor.execute(drop_query)
        print(f"Table {sql_name} dropped successfully.")

        rename_table_query = f"""ALTER TABLE pre_stage.{sql_name}_temp  RENAME TO pre_stage.{sql_name};"""
        cursor.execute(rename_table_query)
        connection.commit()
        print(f"Table {sql_name} renameed successfully.")

    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("Database connection closed.")


def insert_into_db_v1(leads_df, sql_name):
    
    try:
        # Replace NaN with None for MySQL compatibility
        leads_df = leads_df.where(pd.notnull(leads_df), None)

        # Get DB connection
        cursor, connection = get_database_connection()
        cursor.execute("SET GLOBAL max_allowed_packet = 1024 * 1024 * 1024 * 1024;")

        if "Id" in leads_df.columns:
            try:
                alter_table_query = f"""
                    ALTER TABLE pre_stage.{sql_name}
                    MODIFY COLUMN Id VARCHAR(255),
                    ADD UNIQUE KEY uniq_id (Id);
                """
                cursor.execute(alter_table_query)
                connection.commit()
                print(f"'Id' column in table `{sql_name}` altered to VARCHAR(255) and made UNIQUE.")
            except Exception as alter_err:
                print(f"Alter table skipped or failed (likely already modified): {alter_err}")
        else:
            print(f"No 'Id' column in table `{sql_name}`, skipping ALTER.")




        # Prepare insert parts
        columns = leads_df.columns

        column_names = ", ".join(f"`{col}`" for col in columns)
        placeholders = ", ".join(["%s"] * len(columns))

        # Build ON DUPLICATE KEY UPDATE, excluding 'id'
        update_clause = ", ".join(
            f"`{col}` = VALUES(`{col}`)" for col in columns if col != "Id"
        )

        # print(update_clause)
        # update_clause = ", ".join(
        # f"`{col}` = IF(VALUES(`{col}`) <> `{col}`, VALUES(`{col}`), `{col}`)"
        # for col in columns if col.lower() != "id"
        # )

        # Final query
        insert_query = f"""
            INSERT INTO pre_stage.{sql_name} ({column_names})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause};
        """

        # Prepare data tuples
        data_to_insert = [
            tuple(None if pd.isnull(cell) else cell for cell in row)
            for row in leads_df.to_numpy()
        ]
        batch_size = 5000

        # Batch insert
        for i in range(0, len(data_to_insert), batch_size):
            batch = data_to_insert[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()
            print(f"Inserted/Updated batch {i // batch_size + 1}")

        print(f"{len(data_to_insert)} rows inserted/updated successfully in {sql_name}.")

    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("Database connection closed.")


def  ingestion():
    try:

        sf = get_salesforce_connection()

        for key,value in tables_name.items():
            print(f"Describing object: {key}")

            object_to_describe = getattr(sf, key)  # Use dynamic API call
            object_description = object_to_describe.describe()
            fields = [field['name'] for field in object_description['fields']]
            field_names = ", ".join(fields)

            query = f"""
            SELECT {field_names}
            FROM  {key} 
            """
            leads = sf.query_all(query)
            leads_data = [lead for lead in leads['records']]  
            for lead in leads_data:
                lead.pop('attributes', None)
            leads_df = pd.DataFrame(leads_data)
            #drop_and_create_table(value, leads_df)
            insert_into_db_v1(leads_df,value)


    except Exception as e:
        print(f"Error occurred: {e}")



if __name__ == "__main__":
    ingestion()