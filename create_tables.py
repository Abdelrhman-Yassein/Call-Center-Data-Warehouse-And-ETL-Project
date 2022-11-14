# Import Libraries
import mysql.connector
from mysql.connector import Error
# Import Sql Queries
from sql_queries import drop_tables_queries, create_tables_queries


def connect_to_database():
    """_summary_
        - Create New Connection To MySQL Server And DataBase
        - Make sure For Our DataBase
    """
    try:
        # Create New Connection Object To Call_Center Database
        connection = mysql.connector.connect(
            host="localhost", user="root", password="abdyassein", database="call_center", autocommit=True)
        # Check Connection To Database
        if connection.is_connected():
            # Get Server Information
            db_info = connection.get_server_info()
            # Object To Create Cursor Object to perform Various Sql Operation
            cursor = connection.cursor()
            # Run Sql Query and resturn resault
            cursor.execute("SELECT database();")
            # Read Query Result
            database_name = cursor.fetchone()
    except Error as e:
        print("Error : while Connected To MySQL ); ", e)
    return connection, db_info, cursor, database_name


def drop_tables(cursor):
    """_summary_
        Execute Drop Queres To Drop Tables.
    Args:
        cursor (object): Object To Create Cursor Object to Perform Various SQL Operation
    """
    try:
        # iterate Drop Query
        for query in drop_tables_queries:
            # Execute SQL Drop Query
            cursor.execute(query)
    except Error as e:
        print(f"Error : Error While Execute {query} Query : ", e)


def create_tables(cursor):
    """_summary_
        Execute Create Tables To Craete New Tables
    Args:
        cursor (Object): Object To Create Cursor Object To Perform Various SQL Operation
    """
    try:
        # Iterate Create Tables
        for query in create_tables_queries:
            # Eexecute Create Query
            cursor.execute(query)
    except Error as e:
        print(f"Error: Error While Execute {query} Query : ", e)


def main():
    """_summary_
        - Connect To Server And Database
        - Drop Database Tables 
        - Create Database Tables
        - Close Connection And cursor
    """
    print("Welcome To Create Call Center Tables.. ")
    print("Create Connection To Call Center DataBase...")
    connection, db_info, cursor, database_name = connect_to_database()
    print(
        f"Successfully Connected To {db_info} Server And  {database_name} DataBae...")

    print("Execute Queries To Drop All Tables....")
    drop_tables(cursor)
    print("Successfully Drop Tables ):")

    print("Execute Query To Create Tables...")
    create_tables(cursor)
    print("Successfully Create Tables ):")

    print("Close Cursor And Connection... ")
    cursor.close()
    connection.close()
    print("Successfully Create Your DataBase ):")


if __name__ == "__main__":
    main()
