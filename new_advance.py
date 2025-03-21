import mysql.connector
import re

def connect_to_server():
    """Establish connection to MySQL server."""
    return mysql.connector.connect(
        host="localhost",
        user="admin",
        password="Sudhanshu@ojha#8081"
    )

def connect_to_database(database_name):
    """Establish connection to a specific MySQL database."""
    return mysql.connector.connect(
        host="localhost",
        user="admin",
        password="Sudhanshu@ojha#8081",
        database=database_name
    )

def execute_query(connection, query, values=None):
    """Execute a query on the database."""
    print(f"Executing query: {query} with values: {values}")
    cursor = connection.cursor()
    cursor.execute(query, values or ())
    connection.commit()
    return cursor

def list_databases():
    """List all databases in MySQL server."""
    connection = connect_to_server()
    cursor = connection.cursor()
    cursor.execute("SHOW DATABASES")
    databases = [db[0] for db in cursor.fetchall()]
    connection.close()
    return databases

def create_database():
    """Create a new database based on user input."""
    connection = connect_to_server()
    db_name = input("Enter the name of the database you want to create: ")
    query = f"CREATE DATABASE IF NOT EXISTS {db_name}"
    execute_query(connection, query)
    print(f"Database '{db_name}' created successfully.")
    connection.close()
    return db_name

def create_table(connection):
    """Create a new table with user-defined columns."""
    while True:
        table_name = input("Enter the name of the table you want to create: ")
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        existing_tables = [table[0] for table in cursor.fetchall()]

        if table_name in existing_tables:
            print(f"Table '{table_name}' already exists. Please choose a different name.")
        else:
            break

    columns = []
    while True:
        column_name = input("Enter column name (or type 'done' to finish): ")
        if column_name.lower() == 'done':
            break
        column_type = input(f"Enter data type for column '{column_name}' (e.g., INT, VARCHAR(100), DATE): ")
        columns.append(f"`{column_name}` {column_type}")

    columns_query = ", ".join(columns)
    query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns_query})"
    execute_query(connection, query)
    print(f"Table '{table_name}' created successfully.")
    return table_name

def handle_insert(connection, table_name):
    """Handle INSERT operation."""
    cursor = connection.cursor()
    cursor.execute(f"DESCRIBE {table_name}")
    columns = cursor.fetchall()

    values = {}
    for column in columns:
        column_name, column_type = column[0], column[1]
        value = input(f"Enter value for {column_name} ({column_type}): ")
        values[column_name] = value

    columns_query = ", ".join(values.keys())
    placeholders = ", ".join(["%s"] * len(values))
    query = f"INSERT INTO {table_name} ({columns_query}) VALUES ({placeholders})"
    execute_query(connection, query, tuple(values.values()))
    print("Record inserted successfully.")

def handle_update(connection, table_name):
    """Handle UPDATE operation."""
    column_to_update = input("Enter the column to update: ")
    new_value = input(f"Enter the new value for {column_to_update}: ")
    condition_column = input("Enter the column to filter the records: ")
    condition_value = input(f"Enter the value for {condition_column}: ")

    query = f"UPDATE {table_name} SET {column_to_update} = %s WHERE {condition_column} = %s"
    execute_query(connection, query, (new_value, condition_value))
    print("Record updated successfully.")

def handle_delete(connection, table_name):
    """Handle DELETE operation."""
    condition_column = input("Enter the column to filter the records: ")
    condition_value = input(f"Enter the value for {condition_column}: ")

    query = f"DELETE FROM {table_name} WHERE {condition_column} = %s"
    execute_query(connection, query, (condition_value,))
    print("Record deleted successfully.")

def handle_drop(connection, table_name):
    """Handle DROP operation."""
    confirmation = input(f"Are you sure you want to drop the table {table_name}? Type 'YES' to confirm: ")

    if confirmation == "YES":
        query = f"DROP TABLE {table_name}"
        execute_query(connection, query)
        print(f"Table {table_name} dropped successfully.")
    else:
        print("Operation cancelled.")

def handle_make_column(connection, table_name):
    """Handle MAKE COLUMN operation."""
    column_name = input("Enter the name of the new column: ")
    column_type = input("Enter the data type of the new column (e.g., VARCHAR(100), INT, DATE): ")

    query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
    execute_query(connection, query)
    print(f"Column {column_name} added successfully with type {column_type}.")

def main():
    choice = input("If you want to create a whole new database then press 1. If you want to perform DML operations on an old database then press 2: ")

    if choice == "1":
        db_name = create_database()
        connection = connect_to_database(db_name)
        table_name = create_table(connection)

    elif choice == "2":
        databases = list_databases()
        print("Available databases:", ", ".join(databases))
        db_name = input("Enter the name of the database you want to use: ")

        if db_name not in databases:
            print(f"Database '{db_name}' does not exist.")
            return

        connection = connect_to_database(db_name)
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]

        if not tables:
            print(f"No tables found in database '{db_name}'. Please create a table first.")
            table_name = create_table(connection)
        else:
            print("Available tables:", ", ".join(tables))
            table_name = input("Enter the name of the table you want to perform operations on: ")

            if table_name not in tables:
                print(f"Table '{table_name}' does not exist in database '{db_name}'.")
                return

        while True:
            operation = input("Enter the operation you want to perform (INSERT, UPDATE, DELETE, DROP, MAKE_COLUMN, EXIT): ").upper()
            if operation == "INSERT":
                handle_insert(connection, table_name)
            elif operation == "UPDATE":
                handle_update(connection, table_name)
            elif operation == "DELETE":
                handle_delete(connection, table_name)
            elif operation == "DROP":
                handle_drop(connection, table_name)
                break
            elif operation == "MAKE_COLUMN":
                handle_make_column(connection, table_name)
            elif operation == "EXIT":
                print("Exiting DML operations.")
                break
            else:
                print("Invalid operation. Please try again.")

if __name__ == "__main__":
    main()
