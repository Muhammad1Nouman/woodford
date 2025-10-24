import mysql.connector
from datetime import datetime,timedelta
import sys
import logging


def get_database_connection():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            password='Nouman000',
           
           
        )
        cursor = connection.cursor()
        print('Database connection successful')
        return cursor, connection
    except mysql.connector.Error as error:
        print("Error connecting to MySQL database:", error)
        return None, None


def truncate():
    cursor, connection = get_database_connection()
    try:
        truncate_statements = [
                "TRUNCATE TABLE stage.actual_cat_percent;",
                "TRUNCATE TABLE stage.actual_goal_date;",
                "TRUNCATE TABLE stage.actual_prod_cat;",
                "TRUNCATE TABLE stage.customer_funnel;",
                "TRUNCATE TABLE stage.customer_funnel_goal;",
                "truncate table stage.customer_funnel_percentage;"
            ]
        for query in truncate_statements:
                cursor.execute(query)
        connection.commit()
        print("Tables truncated successfully.")
    except Exception as e:
        print(f"Error: {e}")


def insert_into_actual_prod_cat():
    cursor, connection = get_database_connection()  # Ensure this function is defined

    # Fetch data from pre_stage.actual_goal_date
    fetch_query = """SELECT date, type, product_category, leads_taken, leads_nd, 
                            Appointments_Set, Appointments_Cancelled, Appointments_Issued_cc, Appointments_Issued_sale,
                         quote, sold, Cancelled
                     FROM pre_stage.actual_prod_cat;"""
    
    cursor.execute(fetch_query)
    results = cursor.fetchall()  # Fetch all rows

    if not results:
        print("No data found in pre_stage.actual_prod_cat")
        connection.close()
        return
    
    create_query = """
                    CREATE TABLE stage.`temp_actual_prod_cat` (
                    `date` date DEFAULT NULL,
                    `type` varchar(500) DEFAULT NULL,
                    `product_category` varchar(250) DEFAULT NULL,
                    `leads_taken` float DEFAULT NULL,
                    `leads_nd` float DEFAULT NULL,
                    `appointments_set` float DEFAULT NULL,
                    `appointments_cancelled` float DEFAULT NULL,
                    `appointments_issued_cc` float DEFAULT NULL,
                     `appointments_issued_sale` float DEFAULT NULL,
                    `quote_issued` float DEFAULT NULL,
                    `sold` float DEFAULT NULL,
                    `cancelled` float DEFAULT NULL,
                    KEY `product_category` (`product_category`),
                    KEY `date` (`date`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"""
    cursor.execute(create_query)
    connection.commit()
    

    # Insert query for stage.actual_goal_date
    insert_query = """INSERT INTO stage.temp_actual_prod_cat 
                        (date, type, product_category, leads_taken, leads_nd, 
                         appointments_set, appointments_cancelled, appointments_issued_cc, appointments_issued_sale,
                         quote_issued, sold, cancelled)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s);"""
    
    # Insert all fetched rows into stage.actual_goal_date
    cursor.executemany(insert_query, results)
    
    connection.commit()  # Commit changes
    print(f"{cursor.rowcount} rows inserted intotemp_actual_prod_cat")

    drop_table_query = "DROP TABLE IF EXISTS stage.actual_prod_cat;"
    cursor.execute(drop_table_query)
    connection.commit()

    rename_table_query = "ALTER TABLE stage.temp_actual_prod_cat RENAME TO stage.actual_prod_cat;"
    cursor.execute(rename_table_query)
    connection.commit()

    connection.close()  # Close the connection


def insert_into_goal_category():
    cursor, connection = get_database_connection()  # Ensure this function is defined

    # Fetch data from pre_stage.actual_goal_date
    fetch_query = """SELECT date, type, product_category, leads_taken, leads_nd, 
                            Appointments_Set, Appointments_Cancelled, Appointments_Issued_cc, Appointments_Issued_sale,
                            Quote_Issued, sold, Cancelled
                     FROM pre_stage.actual_goal_date;"""
    
    cursor.execute(fetch_query)
    results = cursor.fetchall()  # Fetch all rows

    if not results:
        print("No data found in pre_stage.actual_goal_date")
        connection.close()
        return
    create_query = """
                    CREATE TABLE stage.`temp_actual_goal_date` (
                    `date` date DEFAULT NULL,
                    `type` varchar(500) DEFAULT NULL,
                    `product_category` varchar(250) DEFAULT NULL,
                    `leads_taken` float DEFAULT NULL,
                    `leads_nd` float DEFAULT NULL,
                    `appointments_set` float DEFAULT NULL,
                    `appointments_cancelled` float DEFAULT NULL,
                    `appointments_issued_cc` float DEFAULT NULL,      
                    `appointments_issued_sale` float DEFAULT NULL,               
                    `quote_issued` float DEFAULT NULL,
                    `sold` float DEFAULT NULL,
                    `cancelled` float DEFAULT NULL,
                    KEY `product_category` (`product_category`),
                    KEY `date` (`date`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"""
    cursor.execute(create_query)
    connection.commit()
    # Insert query for stage.actual_goal_date
    insert_query = """INSERT INTO stage.temp_actual_goal_date 
                        (date, type, product_category, leads_taken, leads_nd, 
                         appointments_set, appointments_cancelled, appointments_issued_cc, appointments_issued_sale,
                         quote_issued, sold, cancelled)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s);"""
    
    # Insert all fetched rows into stage.actual_goal_date
    cursor.executemany(insert_query, results)
    
    connection.commit()  # Commit changes
    print(f"{cursor.rowcount} rows inserted into stage.temp_actual_goal_date")

    drop_table_query = "DROP TABLE IF EXISTS stage.actual_goal_date;"
    cursor.execute(drop_table_query)
    connection.commit()

    rename_table_query = "ALTER TABLE stage.temp_actual_goal_date RENAME TO stage.actual_goal_date;"
    cursor.execute(rename_table_query)
    connection.commit()

    connection.close() 



def insert_into_customer_funnel():
    cursor, connection = get_database_connection()  # Ensure this function is defined

    # Fetch data from pre_stage.actual_goal_date
    fetch_query = """SELECT date, Type, Sale_rep, product_category,i360__Source_Name__c,i360__Source_Type__c,Team_Lead,Leads_Taken,Leads_nd, 
	appointments_set, appointments_cancelled, appointments_issued_cc,appointments_issued_sale, 
       Quote_Issued, sold_total, sold, cancelled, cancelled_amount, ADL, ADS
FROM pre_stage.customer_funnel
"""
    
    cursor.execute(fetch_query)
    results = cursor.fetchall()  # Fetch all rows

    if not results:
        print("No data found in pre_stage.customer_funnel")
        connection.close()
        return
    cursor.execute("DROP TABLE IF EXISTS stage.temp_customer_funnel;")
    create_query = """
        CREATE TABLE stage.`temp_customer_funnel` (
        `date` date DEFAULT NULL,
        `type` varchar(500) DEFAULT NULL,
        `Sale_rep` varchar(500) DEFAULT NULL,
        `product_category` varchar(500) DEFAULT NULL,
        `i360__Source_Name__c` varchar(500) DEFAULT NULL,
        `i360__Source_Type__c` varchar(500) DEFAULT NULL,
        `Team_Lead` varchar(500) DEFAULT NULL,
        `leads_taken` float DEFAULT NULL,
        `leads_nd` float DEFAULT NULL,
        `appointments_set` float DEFAULT NULL,
        `appointments_cancelled` float DEFAULT NULL,
        `appointments_issued_cc` float DEFAULT NULL,
        `appointments_issued_sale` float DEFAULT NULL,
        `quote_issued` float DEFAULT NULL,
        `sold_total` float DEFAULT NULL,
        `sale_sold` float DEFAULT NULL,
        `sale_cancelled` float DEFAULT NULL,
        `sale_cancelled_amount` float DEFAULT NULL,
        `ADL` float DEFAULT NULL,
        `ADS` float DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"""
    cursor.execute(create_query)


    # Insert query for stage.actual_goal_date
    insert_query = """INSERT INTO stage.temp_customer_funnel 
                        (date, Type, Sale_rep, product_category,i360__Source_Name__c,i360__Source_Type__c,Team_Lead,Leads_Taken,Leads_nd, 
	appointments_set, appointments_cancelled, appointments_issued_cc,appointments_issued_sale, 
       Quote_Issued, sold_total, sale_sold, sale_cancelled, sale_cancelled_amount, ADL, ADS)
    VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s);"""
    
    # Insert all fetched rows into stage.actual_goal_date
    cursor.executemany(insert_query, results)
    
    connection.commit()  # Commit changes
    print(f"{cursor.rowcount} rows inserted into stage.temp_customer_funnel")

    drop_table_query = "DROP TABLE IF EXISTS stage.customer_funnel;"
    cursor.execute(drop_table_query)
    connection.commit()

    rename_table_query = "ALTER TABLE stage.temp_customer_funnel RENAME TO stage.customer_funnel;"
    cursor.execute(rename_table_query)
    connection.commit()

    connection.close() 





def insert_into_customer_funnel_goal():
    cursor, connection = get_database_connection()  # Ensure this function is defined

    # Fetch data from pre_stage.actual_goal_date
    fetch_query = """SELECT date, type, leads_taken, leads_nd, 
	Appointments_Set, Appointments_Cancelled, appointments_issued_cc, appointments_issued_sale,
       Quote_Issued, sale_sold, sale_Cancelled
FROM pre_stage.goals_by_date
"""
    
    cursor.execute(fetch_query)
    results = cursor.fetchall()  # Fetch all rows

    if not results:
        print("No data found in pre_stage.goals_by_date")
        connection.close()
        return
    
    create_query = """
        CREATE TABLE stage.`temp_customer_funnel_goal` (
                `date` date DEFAULT NULL,
                `type` varchar(500) DEFAULT NULL,
                `leads_taken` float DEFAULT NULL,
                `leads_nd` float DEFAULT NULL,
                `appointments_set` float DEFAULT NULL,
                `appointments_cancelled` float DEFAULT NULL,
                `appointments_issued_cc` float DEFAULT NULL,
                `appointments_issued_sale` float DEFAULT NULL,
                `quote_issued` float DEFAULT NULL,
                `sale_sold` float DEFAULT NULL,
                `sale_cancelled` float DEFAULT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"""
    cursor.execute(create_query)

    # Insert query for stage.actual_goal_date
    insert_query = """INSERT INTO stage.temp_customer_funnel_goal 
                        (date, type, leads_taken, leads_nd, 
	Appointments_Set, Appointments_Cancelled, appointments_issued_cc, appointments_issued_sale,
    Quote_Issued, sale_sold, sale_Cancelled)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s);"""
    
    # Insert all fetched rows into stage.actual_goal_date
    cursor.executemany(insert_query, results)
    
    connection.commit()  # Commit changes
    print(f"{cursor.rowcount} rows inserted into stage.temp_customer_funnel_goal")
    drop_table_query = "DROP TABLE IF EXISTS stage.customer_funnel_goal;"
    cursor.execute(drop_table_query)
    connection.commit()

    rename_table_query = "ALTER TABLE stage.temp_customer_funnel_goal RENAME TO stage.customer_funnel_goal;"
    cursor.execute(rename_table_query)
    connection.commit()
    

    

    connection.close() 


def insert_into_vtc():
    cursor, connection = get_database_connection() 
    insert_query ="""
            INSERT INTO `pre_stage`.`vtc_dates` (`date`, `vtc`)
            SELECT
                CURDATE() AS date,
                SUM(Value_To_Complete_VTC__c) AS vtc
            FROM pre_stage.project
            WHERE
                Percentage_of_Completion__c < 100
                AND i360__Status__c = 'Active'
                AND COALESCE(i360__Job_Type__c, '') <> '021 - Service Work'
            ON DUPLICATE KEY UPDATE
                vtc = VALUES(vtc);"""
    cursor.execute(insert_query)
    connection.commit()
    print("VTC added")
    



def transformation():
    # truncate()
    insert_into_actual_prod_cat()
    insert_into_goal_category()
    insert_into_customer_funnel()
    insert_into_vtc()
    # insert_into_customer_funnel_goal()
   



if __name__ == "__main__":
    transformation()






