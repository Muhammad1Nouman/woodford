
import openpyxl
import mysql.connector

def get_database_connection():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
            host='10.0.0.75',
            port=3306,
            user='pbiRefresh',
            password='pbiRefresh',
            database='pre_stage'
        )
        cursor = connection.cursor()
        print('Database connection successful')
        return cursor, connection
    except mysql.connector.Error as error:
        print("Error connecting to MySQL database:", error)
        return None, None

def update_crew_weeks():
    try:
        # Source file path
        source_file_path = r'D:\Power BI\woodfordbros.com\Woodford All - Power BI Files\00 - Production Scheduling for the Week.xlsx'

        # Load workbook and sheet
        source_workbook = openpyxl.load_workbook(source_file_path, data_only=True)
        scheduling_sheet = source_workbook['Scheduling Sheet']

        # Extract values
        crew_weeks = scheduling_sheet['D40'].value
        week_date = scheduling_sheet['B5'].value

        print(f"Crew Weeks (D40): {crew_weeks}")
        print(f"Week Date (B5): {week_date}")

        cursor, connection = get_database_connection()

        if cursor and connection:
            cursor.execute("SELECT crew_worked FROM pre_stage.weekly_crew_logs WHERE date = %s", (week_date,))
            result = cursor.fetchone()

            if result is None:
                cursor.execute("INSERT INTO pre_stage.weekly_crew_logs (date, crew_worked) VALUES (%s, %s)", (week_date, crew_weeks))
                print("Inserted new record into weekly_crew_logs.")
            else:
                existing_crew_weeks = result[0]
                if existing_crew_weeks != crew_weeks:
                    cursor.execute("UPDATE pre_stage.weekly_crew_logs SET crew_worked = %s WHERE date = %s", (crew_weeks, week_date))
                    print("Updated existing record in weekly_crew_logs.")

            connection.commit()
            cursor.close()
            connection.close()
            print("Database connection closed.")
    except Exception as e:
        print("Error in update_crew_weeks:", e)

# Optional: Allow direct standalone execution
if __name__ == "__main__":
    update_crew_weeks()