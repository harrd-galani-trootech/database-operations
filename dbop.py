import psycopg2
import mysql.connector
import getpass
import re
from time import sleep
import logging
import traceback
from tabulate import tabulate
MAIN = '__main__'
ENTER_VALID_OPTION = "Please enter a valid option: "
PRESS_ENTER_TO_GO_BACK = "\nPress just enter to go back.\n"
ENTER_DATABASE_NAME = PRESS_ENTER_TO_GO_BACK + "Enter the name for your database: "
ENTER_USER_NAME =  PRESS_ENTER_TO_GO_BACK + "Enter the user name: "
ENTER_YOUR_PASSWORD =  PRESS_ENTER_TO_GO_BACK + "Enter your password: "
ENTER_HOST = PRESS_ENTER_TO_GO_BACK + "Enter host address: "
OR = 'or'
ENTER_YOUR_CHOICE = 'Enter your choice:\n'
NEW_LINE = '\n'
DBMS_MENU = \
"""
*****DBMS MENU*****
1) PostgreSQL
2) MySQL
Press just enter to exit
""" + ENTER_YOUR_CHOICE
PG_DB_MENU = \
"""
*****PostgreSQL Database Menu*****
1) Create a new user
2) Create a new database
3) Show all databases
4) Use your existing database
5) Delete a database
6) Show all users
7) Delete a user""" + PRESS_ENTER_TO_GO_BACK + ENTER_YOUR_CHOICE
MY_DB_MENU = \
"""
*****MySQL Database Menu*****
1) Create a new user
2) Create a new database
3) Show all databases
4) Use your existing database
5) Delete a database
6) Show all users
7) Delete a user""" + PRESS_ENTER_TO_GO_BACK + ENTER_YOUR_CHOICE
PG_TABLE_MENU = \
"""
*****PostgreSQL Table Menu*****
1) Create a new table
2) Describe a table
3) Alter a table
4) Delete a table
5) Show all tables
6) Use an existing table""" + PRESS_ENTER_TO_GO_BACK + ENTER_YOUR_CHOICE
MY_TABLE_MENU = \
"""
*****MySQL Table Menu*****
1) Create a new table
2) Describe a table
3) Alter a table
4) Delete a table
5) Show all tables
6) Use an existing table""" + PRESS_ENTER_TO_GO_BACK + ENTER_YOUR_CHOICE
MY_RECORD_MENU = \
"""
*****MySQL Record Menu*****
1) Insert a new record
2) Show all records
3) Show records with conditions
4) Update a record
5) Delete a record""" + PRESS_ENTER_TO_GO_BACK + ENTER_YOUR_CHOICE
PG_RECORD_MENU = \
"""
*****PostgreSQL Record Menu*****
1) Insert a new record
2) Show all records
3) Show records with conditions
4) Update a record
5) Delete a record""" + PRESS_ENTER_TO_GO_BACK + ENTER_YOUR_CHOICE
ENTER_EXISTING_DATABASE_NAME= "Enter name of your existing database: "
DATABASE_DOES_NOT_EXIST = "Database does not exist!"
DATABASE_ACCESS_SUCCESSFUL = "Database access successful"
PG_USER_CREATED = "User for postgres created successfully!"
MY_USER_CREATED = "User for mysql created successfully!"
ENTER_TABLE_NAME = "Enter table name: "
ENTER_NO_OF_COLS = "Enter number of columns: "
PASS_MIN_8_CHARS = "Your password should be of minimum 8 characters." \
    "Please try again."
PASS_MIXED_COUNT = "Your password should have atleast 1 mixed case count." \
    "Please try again."
PASS_ATLEAST_1_DIGIT = "Your password should contain atleast 1 digit." \
    "Please try again."
PASS_ATLEAST_1_SPECIAL_CHAR = "Your password should contain atleast 1 special character." \
    "Please try again."
CONFIRM_YOUR_PASSWORD = 'Confirm your password: '
CONFIRMATION_PASSWORD_DIDNT_MATCH = \
    "Your password didnt match the confirmation password. Please try again."
SPECIAL_CHARS = "[@_!#$%^&*()<>?/\|}{~:]"
PG_QUERY_SHOW_ALL_USERS = """
SELECT usename AS role_name,
  CASE 
     WHEN usesuper AND usecreatedb THEN 
	   CAST('superuser, create database' AS pg_catalog.text)
     WHEN usesuper THEN 
	    CAST('superuser' AS pg_catalog.text)
     WHEN usecreatedb THEN 
	    CAST('create database' AS pg_catalog.text)
     ELSE 
	    CAST('' AS pg_catalog.text)
  END role_attributes
FROM pg_catalog.pg_user
ORDER BY role_name desc;
"""
MY_QUERY_SHOW_ALL_USERS = 'SELECT user, host FROM mysql.user;'
SQL_IDENTIFIER_NAMING_RULES = """
SQL identifiers and key words must begin with a letter (a-z, but also letters with 
diacritical marks and non-Latin letters) or an underscore (_). Subsequent characters 
in an identifier or key word can be letters, underscores, digits (0-9), or dollar 
signs ($).
"""
PLEASE_TRY_AGAIN = 'Please try again'
USER_NAME_INVALID = 'Username is invalid.'
USER_DOES_NOT_EXIST = 'User does not exist.'
CREDENTIALS_INCORRECT = 'Credentials are incorrect.'
USER_NAME_OR_DATABASE_INVALID = 'Username or database is invalid.'
DETAILS_INCORRECT = '\nDetails are incorrect.\n'
DETAILS_INVALID = '\nDetails are invalid.\n'
TABLE_DOES_NOT_EXIST = 'Table does not exist.'
TABLE_ALREADY_EXISTS = 'Table already exists.'
VALUES_INVALID = 'Some values do not correspond to the datatype of their ' \
                 'respective columns'
COLUMN_DOES_NOT_EXIST = 'Column does not exist.'
ENTER_A_CONDITION = 'Enter a condition: '




class DbOp:
    """
        Operations on Database, Tables and Records in MySQL and PostgreSQL
    """
    def __init__(self) -> None:
        """
            initilializes attributes and calls the dbms menu
        """
        self.no_of_columns_in_a_table = None
        self.table = None
        self.database = None
        self.password = None
        self.username = None
        self.connection = None
        self.cursor = None
        self.dbms_menu()

    def dbms_menu(self) -> None:
        """
*****DBMS MENU*****
1) PostgreSQL
2) MySQL
"""
        while True:
            option = input(DBMS_MENU).strip()
            if option == '1':
                self.pg_db_menu()
            elif option == '2':
                self.my_db_menu()
            elif option == '':
                break
            else:
                print(ENTER_VALID_OPTION)


    def pg_db_menu(self) -> None:
        """
*****PostgreSQL Menu*****
1) Create a new user
2) Create a new database
3) Show all databases
4) Use your existing database
5) Delete a database
6) Show all users
7) Delete a user
"""
        while True:
            option = input(PG_DB_MENU).strip()
            if option == '1':
                self.pg_create_a_new_user()
            elif option == '2':
                self.pg_create_a_new_database()
            elif option == '3':
                self.pg_show_all_databases()
            elif option == '4':
                self.pg_use_a_database()
            elif option == '5':
                self.pg_delete_a_database()
            elif option == '6':
                self.pg_show_all_users()
            elif option == '7':
                self.pg_delete_a_user()
            elif option == '':
                break
            else:
                print(ENTER_VALID_OPTION)


    def my_db_menu(self) -> None:
        """
1) Create a new user
2) Create a new database
3) Show all databases
4) Use your existing database
5) Delete a database
6) Show all users
7) Delete a user
"""
        while True:
            option = input(MY_DB_MENU).strip()
            if option == '1':
                self.my_create_a_new_user()
            elif option == '2':
                self.my_create_a_new_database()
            elif option == '3':
                self.my_show_all_databases()
            elif option == '4':
                self.my_use_a_database()
            elif option == '5':
                self.my_delete_a_database()
            elif option == '6':
                self.my_show_all_users()
            elif option == '7':
                self.my_delete_a_user()
            elif option == '':
                break
            else:
                print(ENTER_VALID_OPTION)

    def pg_table_menu(self) -> None:
        """
*****PostgreSQL Table Menu*****
1) Create a new table
2) Describe a table
3) Alter a table
4) Delete a table
5) Show all tables
6) Use an existing table
"""
        while True:
            option = input(PG_TABLE_MENU).strip()
            if option == "1":
                self.pg_create_a_new_table()
            elif option == '2':
                self.pg_describe_a_table()
            elif option == '3':
                self.pg_alter_a_table()
            elif option == '4':
                self.pg_drop_a_table()
            elif option == '5':
                self.pg_show_all_tables()
            elif option == '6':
                self.pg_use_an_existing_table()
            elif option == '':
                break
            else:
                print(ENTER_VALID_OPTION)

    def my_table_menu(self) -> None:
        """
*****MySQL Table Menu*****
1) Create a new table
2) Describe a table
3) Alter a table
4) Delete a table
5) Show all tables
6) Use an existing table
"""
        while True:
            option = input(MY_TABLE_MENU).strip()
            if option == "1":
                self.my_create_a_new_table()
            elif option == '2':
                self.my_describe_a_table()
            elif option == '3':
                self.my_alter_a_table()
            elif option == '4':
                self.my_drop_a_table()
            elif option == '5':
                self.my_show_all_tables()
            elif option == '6':
                self.my_use_an_existing_table()
            elif option == '':
                break
            else:
                print(ENTER_VALID_OPTION)

    def my_record_menu(self) -> None:
        """*****MySQL Record Menu*****
1) Insert a new record
2) Show all records
3) Show records with conditions
4) Update a record
5) Delete a record
"""
        while True:
            option = input(MY_RECORD_MENU).strip()
            if option == '1':
                self.my_record_insert_a_new_record()
            elif option == '2':
                self.my_record_show_all_records()
            elif option == '3':
                self.my_record_show_records_with_conditions()
            elif option == '4':
                self.my_record_update_a_record()
            elif option == '5':
                self.my_record_delete_a_record()
            elif option == '':
                break
            else:
                print(ENTER_VALID_OPTION)


# postgres database level operations
    def pg_create_a_new_user(self) -> None:
        """
        Creates a new user.
        """
        while True:
            self.input_new_username()
            if self.username  == '': break

            self.input_new_password()
            if self.password == '': break

            try:
                pg_super_cursor.execute(f"CREATE USER {self.username} WITH PASSWORD '{self.password}';")
            except psycopg2.errors.DuplicateObject:
                print(f"User {self.username} already exists. Please try to enter difference username")
            except psycopg2.errors.SyntaxError:
                print(SQL_IDENTIFIER_NAMING_RULES, USER_NAME_INVALID)
            else:
                print(PG_USER_CREATED)
                break
    
    def pg_create_a_new_database(self) -> None:
        """
        Creates a new database.
        """
        while True:
            self.input_username()
            if self.username  == '': break

            self.input_password()
            if self.password == '': break

            self.input_new_database()
            if self.database == '': break

            try:
                pg_super_cursor.execute(f"CREATE DATABASE {self.database} WITH OWNER '{self.username}';")
            except psycopg2.errors.SyntaxError:
                print(SQL_IDENTIFIER_NAMING_RULES, ' ', USER_NAME_OR_DATABASE_INVALID) 
            except psycopg2.errors.UndefinedObject:
                print(USER_DOES_NOT_EXIST)
            except psycopg2.errors.DuplicateDatabase:
                print(f'Database {self.database} already exists.')
            else:
                try:
                    psycopg2.connect(
                        user=self.username,
                        password=self.password,
                        database=self.database,
                        host='localhost'
                    )
                    print(f"Database '{self.database}' created successfully!")
                    break
                except psycopg2.OperationalError:
                    print(CREDENTIALS_INCORRECT)

    def pg_show_all_databases(self) -> None:
        """
        Displays all databases.
        """
        pg_super_cursor.execute("SELECT datname FROM pg_database;")
        db_list = pg_super_cursor.fetchall()
        output = ', '.join(map(lambda item: item[0], db_list))
        print(output)

    def pg_delete_a_database(self) -> None:
        """
        Deletes a database.
        """
        while True:
            self.my_show_all_databases()
            self.input_database()
            if self.database == '': break
            try:
                pg_super_cursor.execute(f"DROP DATABASE {self.database}")
            except psycopg2.errors.SyntaxError:
                print(DETAILS_INCORRECT, NEW_LINE)
            except psycopg2.errors.InvalidCatalogName:
                print(f'Database {self.database} does not exist.', NEW_LINE)
            else:
                print(f'Database {self.database} deleted successfully.')
                break

    def pg_use_a_database(self) -> None:
        """
        Access an existing database for further table and row operations.
        """        
        while True:
            self.pg_show_all_databases()
            self.input_username()
            if self.username == '': break
            self.input_password()
            if self.password == '': break
            self.input_database()
            if self.database == '': break

            try:
                self.connection = psycopg2.connect(
                    user=self.username,
                    password=self.password,
                    database=self.database,
                    host='localhost'
                )
                self.connection.autocommit = True
                self.cursor = self.connection.cursor()
            except psycopg2.OperationalError:
                print(DETAILS_INCORRECT, NEW_LINE)
            else:
                print(f"\nYou are now using database '{self.database}'")
                self.pg_table_menu()
                self.cursor.close()
                self.connection.close()
                break

    def pg_show_all_users(self) -> None:
        """
        Displays all users.
        """
        pg_super_cursor.execute(PG_QUERY_SHOW_ALL_USERS.strip())
        user_list = pg_super_cursor.fetchall()
        output = ', '.join(map(lambda item: item[0], user_list))
        print(output)

    def pg_delete_a_user(self) -> None:
        """
            delete a user
        """
        while True:
            self.pg_show_all_users()
            self.input_username()
            if self.username == '': break
            try:
                pg_super_cursor.execute(f"DROP USER {self.username};")
            except psycopg2.errors.SyntaxError:
                print(DETAILS_INCORRECT)
            except psycopg2.errors.UndefinedObject:
                print(USER_DOES_NOT_EXIST)
            except psycopg2.errors.DependentObjectsStillExist:
                print(f'{self.username} ' 
                      'Cannot be deleted because some objects depend on it')
            else:
                print(f'User {self.username} deleted successfully.')
                break

# mysql database level operations
    def my_create_a_new_user(self) -> None:
        """
        Creates a new user.
        """        
        while True:
            self.input_new_username()
            if self.username == '': break
            
            self.input_new_password()
            if self.password == '': break

            try:
                my_super_cursor.execute(f"CREATE USER '{self.username}'@'localhost' IDENTIFIED BY '{self.password}';")
            except mysql.connector.errors.DatabaseError:
                print(f"User {self.username} already exists. Please try to enter difference username")
            else:
                print(MY_USER_CREATED)
                break
    
    def my_create_a_new_database(self) -> None:
        """
        Creates a new database.
        """        
        while True:
            self.input_username()
            if self.username == '': break
            self.input_password()
            if self.password == '': break
            
            self.input_new_database()
            if self.database == '': break

            try:
                my_super_cursor.execute(f"CREATE DATABASE {self.database};")
                my_super_cursor.execute(f"grant usage on *.* to {self.username}@localhost;")
                my_super_cursor.execute(f"grant all privileges on {self.database}.* to {self.username}@localhost WITH GRANT OPTION;")
                my_super_cursor.execute(f"flush privileges;")
                mysql.connector.connect(
                    user=self.username,
                    password=self.password,
                    database=self.database,
                    host='localhost'
                )
            except mysql.connector.errors.ProgrammingError:
                print(CREDENTIALS_INCORRECT)
            except mysql.connector.errors.DatabaseError:
                print(f"Can't create database '{self.database}';"
                       " Either database already exists"
                       " or database name is invalid."
                       " Please try again.")

            else:
                print(f"Database '{self.database}' created successfully!")
                break


    def my_show_all_databases(self) -> None:
        """
        Displays all databases.
        """        
        my_super_cursor.execute("SHOW DATABASES;")
        db_list = my_super_cursor.fetchall()
        output = ', '.join(map(lambda item: item[0], db_list))
        print(output)
    
    def my_delete_a_database(self) -> None:
        """
        Deletes a database.
        """        
        while True:
            self.my_show_all_databases()
            self.input_database()
            if self.database == '': break
            try:
                my_super_cursor.execute(f"DROP DATABASE {self.database};")
            except mysql.connector.errors.ProgrammingError:
                print(DETAILS_INCORRECT)
            except mysql.connector.errors.DatabaseError:
                print(f"Can't drop database '{self.database}'; database doesn't exist")      
            else:
                print(f'Database {self.database} deleted successfully.')
                break
    
    def my_use_a_database(self) -> None:
        """
        Access an existing database for further table and row operations.
        """
        while True:
            self.my_show_all_databases()
            self.input_username()
            if self.username == '': break
            self.input_password() 
            if self.password == '': break
            self.input_database()
            if self.database == '': break
            try: 
                self.connection = mysql.connector.connect(
                    user=self.username,
                    password=self.password,
                    database=self.database,
                    host='localhost'
                )
                self.connection.autocommit = True
                self.cursor = self.connection.cursor(buffered=True)
            except mysql.connector.errors.ProgrammingError:
                print(DETAILS_INCORRECT, NEW_LINE)
            else:
                print(f"\nYou are now using database '{self.database}'")
                self.my_table_menu()
                self.cursor.close()
                self.connection.close()
                break
    
    def my_show_all_users(self) -> None:
        """
        Displays all users.
        """
        my_super_cursor.execute(MY_QUERY_SHOW_ALL_USERS.strip())
        user_list = my_super_cursor.fetchall()
        output = ', '.join(map(lambda item: item[0], user_list))
        print(output)
    
    def my_delete_a_user(self) -> None:
        while True:
            self.my_show_all_users()
            self.input_username()
            if self.username == '': break
            try:
                my_super_cursor.execute(
                    f"DROP USER '{self.username}'@'localhost';")    
            except mysql.connector.errors.ProgrammingError:
                print(DETAILS_INCORRECT)
            except mysql.connector.errors.DatabaseError:
                print(USER_DOES_NOT_EXIST, NEW_LINE)

# postgres table level operations
    def pg_create_a_new_table(self) -> None:
        """
            Creates a new postgres table
        """
        while True:
            try:
                self.input_new_table()
                if self.table == '': break

                query = f"CREATE TABLE {self.table}("
                try:
                    num_of_cols = int(input(ENTER_NO_OF_COLS))
                    if num_of_cols <= 0: raise ValueError
                except ValueError:
                    print('Value should be a positive integer only.')
                    continue
                for index in range(1, num_of_cols+1):
                    col_name = input(f'Enter name for column {index}: ')
                    datatype = input(f'Enter datatype for column {index}: ')
                    col_constraints = \
                        input(f'Enter column constraints for column {index}:')
                    query += f'\n\t{col_name} {datatype} {col_constraints}'
                    if index != num_of_cols:
                        query += ','
                table_constraints = input('Enter table constraints: ')
                query += f'\t{table_constraints}\n'
                query += ');'
                print(query)
                self.cursor.execute(query)
            except (psycopg2.errors.SyntaxError, psycopg2.errors.UndefinedObject):
                print('Enter valid details.', NEW_LINE)
            except psycopg2.errors.DuplicateTable:
                print('Table already exists.', NEW_LINE)
            else:
                print(f'Table {self.table} created successfully.', NEW_LINE)
                break
    
    def pg_describe_a_table(self) -> None:
        """
            describes a table
        """
        while True:
            self.pg_show_all_tables()
            self.input_table()
            if self.table == '': break
            self.cursor.execute(f"SELECT column_name, data_type, is_nullable, column_default\
                FROM information_schema.columns\
                WHERE table_name = '{self.table}';")
            description = self.cursor.fetchall()
            self.cursor.execute(f"SELECT indexname\
                FROM pg_indexes \
                WHERE tablename = '{self.table}'\
                ORDER BY indexname; ")
            indexes = self.cursor.fetchall()
            if not description:
                print("Enter correct table name.")
                continue

            else:
                columns = ['column_name', 'data_type', 'is_nullable', 'column_default']
                print(tabulate(description, headers=columns))
                print(indexes)
                break
    
    def pg_alter_a_table(self) -> None:
        pass
    
    def pg_drop_a_table(self) -> None:
        """
            deletes a table
        """
        while True:
            self.pg_show_all_tables()
            self.input_table()
            if self.table == '': break
            try:
                self.cursor.execute(f'DROP TABLE {self.table}')
            except psycopg2.errors.UndefinedTable:
                print(f"Table '{self.table}' does not exist.", NEW_LINE)
            except psycopg2.errors.SyntaxError:
                print(DETAILS_INVALID, NEW_LINE)
            else:
                print(f"Table '{self.table}' deleted successfully.", NEW_LINE)
                break

    def pg_show_all_tables(self) -> None:
        """
            show all tables
        """
        self.cursor.execute("SELECT * FROM pg_catalog.pg_tables "
            "WHERE schemaname != 'pg_catalog' AND "
            "schemaname != 'information_schema';")
        table_list = self.cursor.fetchall()
        output = ', '.join(map(lambda item: item[1], table_list))
        print(output)
    
    def pg_use_an_existing_table(self) -> None:
        """
            use an existing table
        """
        while True:
            self.pg_show_all_tables()
            self.input_table()
            if self.table == '': break
            try:
                query = f'SELECT * FROM {self.table}'
                self.cursor.execute(query)
            except psycopg2.errors.SyntaxError:
                print(DETAILS_INVALID)
            except psycopg2.errors.UndefinedTable:
                print(TABLE_DOES_NOT_EXIST)
            else:
                print(f"\nYou are now using table '{self.table}'.")
                self.pg_record_menu()
                break

# mysql table level operations
    
    def my_create_a_new_table(self) -> None:
        """
            create a new table
        """
        while True:
            self.input_new_table()
            if self.table == '': break
            query = f"CREATE TABLE {self.table}("
            try:
                num_of_cols = int(input(ENTER_NO_OF_COLS))
                if num_of_cols <= 0: raise ValueError
            except ValueError:
                print('Value should be a positive integer only.')
                continue
            for index in range(1, num_of_cols+1):
                col_name = input(f'Enter name for column {index}: ')
                datatype = input(f'Enter datatype for column {index}: ')
                col_constraints = \
                    input(f'Enter column constraints for column {index}:')
                query += f'\n\t{col_name} {datatype} {col_constraints}'
                if index != num_of_cols:
                    query += ','
            table_constraints = input('Enter table constraints: ')
            query += f'\t{table_constraints}\n'
            query += ');'
            print(query)
            try:
                self.cursor.execute(query)
            except mysql.connector.errors.ProgrammingError:
                print(DETAILS_INVALID, OR, TABLE_ALREADY_EXISTS, PLEASE_TRY_AGAIN, 
                      NEW_LINE)
            else:
                print(f'Table {self.table} created successfully!')
                break

    def my_describe_a_table(self) -> None:
        """
            describe a table
        """
        while True:
            self.my_show_all_tables()
            self.input_table()
            if self.table == '': break
            try:
                query = f'DESCRIBE {self.table};'
                description = self.execute_a_table_or_record_query_and_return_output(query)
            except mysql.connector.errors.ProgrammingError:
                print(DETAILS_INVALID, OR, TABLE_DOES_NOT_EXIST, PLEASE_TRY_AGAIN, 
                      NEW_LINE)
            else:
                columns = ['Field', 'Type', 'Null', 'Key', 'Default', 'Extra']
                print(tabulate(description, headers=columns))
                break

    def my_alter_a_table(self) -> None:
        pass
    
    def my_drop_a_table(self) -> None:
        """
            delete a table
        """
        while True:
            self.my_show_all_tables()            
            self.input_table()
            if self.table == '': break
            try:
                self.cursor.execute(f'DROP TABLE {self.table}')
            except mysql.connector.errors.ProgrammingError:
                print(DETAILS_INVALID, OR, TABLE_ALREADY_EXISTS, PLEASE_TRY_AGAIN, 
                      NEW_LINE)            
            else:
                print(f"Table '{self.table}' deleted successfully.", NEW_LINE)
                break

    def my_show_all_tables(self) -> None:
        """
            show all tables
        """
        table_list = self.execute_a_table_or_record_query_and_return_output('SHOW TABLES;')
        output = ', '.join(map(lambda item: item[0], table_list))        
        print(output)

    def my_use_an_existing_table(self) -> None:
        """
            use an existing table
        """
        while True:
            self.my_show_all_tables()
            self.input_table()
            if self.table == '': break
            query = f'SELECT * FROM {self.table}'
            try:
                self.cursor.execute(query)
            except mysql.connector.errors.ProgrammingError:
                print(DETAILS_INVALID, OR, TABLE_DOES_NOT_EXIST, PLEASE_TRY_AGAIN, 
                        NEW_LINE)                
            else:
                print(f"\nYou are now using table '{self.table}'.")
                self.my_record_menu()
                break

# postgres record level operations:
    def pg_record_menu(self) -> None:
        """*****PostgreSQL Record Menu*****
        1) Insert a new record
        2) Show all records
        3) Show records with conditions
        4) Update a record
        5) Delete a record
        """
        while True:
            option = input(PG_RECORD_MENU).strip()
            if option == '1':
                self.pg_record_insert_a_new_record()
            elif option == '2':
                self.pg_record_show_all_records()
            elif option == '3':
                self.pg_record_show_records_with_conditions()
            elif option == '4':
                self.pg_record_update_a_record()
            elif option == '5':
                self.pg_record_delete_a_record()
            elif option == '':
                break
            else:
                print(ENTER_VALID_OPTION)

    def pg_record_insert_a_new_record(self) -> None:
        """
        Inserts a new record.
        """
        while True:
            self.get_no_of_columns_in_a_table()
            query = f"INSERT INTO {self.table}\nVALUES ("
            for index in range(1, self.no_of_columns_in_a_table + 1):
                val = input(f"Value for column {index}: ").strip()
                query += f"'{val}', "
            query = query[:-2]
            query+=');'
            print(query)
            try:
                self.cursor.execute(query)
            except psycopg2.DataError:
                print(VALUES_INVALID)
            print('Record inserted')
            break
    
    def pg_record_show_all_records(self) -> None:
        """
        Displays all records of a table.
        """        
        query = f"SELECT * FROM {self.table};"
        output = self.execute_a_table_or_record_query_and_return_output(query)
        query = f"SELECT column_name\
                 FROM information_schema.columns\
                 WHERE table_name = '{self.table}';"
        description = \
            self.execute_a_table_or_record_query_and_return_output(query)
        columns = list(map(lambda item: item[0], description))
        print(tabulate(output, headers=columns))

    def pg_record_show_records_with_conditions(self) -> None:
        """
        Show records from a table that satisfies a given condition.
        """        
        while True:
            query = f"SELECT * FROM {self.table}\n"
            condition = input(ENTER_A_CONDITION)
            query = query + f'WHERE {condition};'
            try:
                output = self.execute_a_table_or_record_query_and_return_output(query)
            except psycopg2.errors.SyntaxError:
                print(DETAILS_INVALID)
            except psycopg2.errors.UndefinedColumn:
                print(DETAILS_INCORRECT)
            else:
                print(output)
                break
    
    def pg_record_update_a_record(self) -> None:
        """
        Updates a record 
        """        
        while True:
            self.pg_record_show_all_records()
            query = f"UPDATE {self.table}\nSET"
            print(PRESS_ENTER_TO_GO_BACK)
            col_name = input("Enter a col name to be updated: ")
            if col_name == '': break
            val = input("Enter a new value: ")
            if val == '': break
            query += f" {col_name} = '{val}',"
            query = query[:-1]
            condition = input(ENTER_A_CONDITION)
            if condition == '': break
            query+=f'\nWHERE {condition}\nRETURNING *;'
            try:
                output = self.execute_a_table_or_record_query_and_return_output(query)
                print(output)
            except psycopg2.errors.UndefinedFunction:
                print(VALUES_INVALID, NEW_LINE)
            except psycopg2.errors.SyntaxError:
                print(DETAILS_INVALID, NEW_LINE)
            except psycopg2.DataError:
                print(VALUES_INVALID, NEW_LINE)
            else:
                print('Record updated successfully!')
                break

    def pg_record_delete_a_record(self) -> None:
        """
        Deletes a record.
        """        
        while True:
            self.pg_record_show_all_records()
            condition = input(ENTER_A_CONDITION)
            query = f"DELETE FROM {self.table} WHERE {condition};"
            try:
                self.cursor.execute(query)
            except psycopg2.errors.UndefinedFunction:
                print(VALUES_INVALID, NEW_LINE)
            except psycopg2.errors.SyntaxError:
                print(DETAILS_INVALID, NEW_LINE)
            except psycopg2.DataError:
                print(VALUES_INVALID, NEW_LINE)
            else:
                print('Record deleted successfully!')       
                break

# mysql record level operations:
    def my_record_insert_a_new_record(self) -> None:
        """
        Inserts a new record.
        """
        while True:
            self.get_no_of_columns_in_a_table()
            query = f"INSERT INTO {self.table}\nVALUES ("

            for index in range(1, self.no_of_columns_in_a_table + 1):
                val = input(f"Value for column {index}: ").strip()   
                query += f"{val}, "
            query = query[:-2]
            query+=');'
            try:
                self.cursor.execute(query)
            except mysql.connector.errors.DataError:
                print(VALUES_INVALID, PLEASE_TRY_AGAIN, NEW_LINE)
            except mysql.connector.errors.ProgrammingError:
                print(DETAILS_INVALID, PLEASE_TRY_AGAIN, NEW_LINE)
            else:
                print('Record inserted successfully!')
                break
    
    def my_record_show_all_records(self) -> None:
        """
        Displays all records of a table.
        """        
        query = f"SELECT * FROM {self.table};"
        output = self.execute_a_table_or_record_query_and_return_output(query)
        query = f'DESCRIBE {self.table};'
        description = \
            self.execute_a_table_or_record_query_and_return_output(query)
        columns = list(map(lambda item: item[0], description))
        print(tabulate(output, headers=columns))
    
    def my_record_show_records_with_conditions(self) -> None:
        """
        Show records from a table that satisfies a given condition.
        """
        while True:
            query = f"SELECT * FROM {self.table}\n"
            condition = input(ENTER_A_CONDITION)
            if condition == '': break
            query = query + f'WHERE {condition};'
            try:
                output = self.execute_a_table_or_record_query_and_return_output(query)
            except mysql.connector.errors.ProgrammingError:
                print(DETAILS_INVALID, OR, DETAILS_INCORRECT)
            else:
                print(output)
                break
    
    def my_record_update_a_record(self) -> None:
        """
        Updates a record 
        """
        while True:
            self.my_record_show_all_records()
            query = f"UPDATE {self.table}\nSET"
            print('Press enter to break')
            break_flag = False
            while True:
                col_name = input("Enter a col name to be updated: ")
                if col_name == '':
                    break_flag = True
                    break
                val = input("Enter a new value: ")
                if val == '':
                    break_flag = True
                    break
                query += f" {col_name} = {val},"
            if break_flag:
                break
            query = query[:-1]
            condition = input(ENTER_A_CONDITION)
            query += f"\nWHERE {condition};"
            try:
                output = self.execute_a_table_or_record_query_and_return_output(query)
                print(output)
            except mysql.connector.errors.DataError:
                print(VALUES_INVALID, PLEASE_TRY_AGAIN, NEW_LINE)
            except mysql.connector.errors.ProgrammingError:
                print(DETAILS_INVALID, OR, COLUMN_DOES_NOT_EXIST, PLEASE_TRY_AGAIN, NEW_LINE)
            else:
                print('Record updated successfully!')
                break            
    
    def my_record_delete_a_record(self) -> None:
        """
        Deletes a record.
        """
        while True:
            self.my_record_show_all_records()
            condition = input(ENTER_A_CONDITION)
            if condition == '': break
            query = f"DELETE FROM {self.table} WHERE {condition};"
            try:
                output = self.execute_a_table_or_record_query_and_return_output(query)
                print(output)
            except mysql.connector.errors.ProgrammingError:
                print(DETAILS_INVALID, OR, COLUMN_DOES_NOT_EXIST, NEW_LINE)
            except mysql.connector.errors.DataError:
                print(VALUES_INVALID, NEW_LINE)

# methods to input names 
    def input_username(self) -> None:
        """
        Inputs username.
        """
        new_username = input(ENTER_USER_NAME).strip()
        self.username = new_username
    
    def input_password(self) -> None:
        """
        Inputs password.
        """
        password = getpass.getpass(ENTER_YOUR_PASSWORD)
        self.password = password
    
    def input_database(self) -> None:
        """
        Inputs database name
        """
        database = input(ENTER_DATABASE_NAME).strip()
        self.database = database
    
    def input_new_username(self) -> None:
        """
        Input new username
        """
        new_username = input(ENTER_USER_NAME).strip()
        self.username = new_username
    
    def input_new_password(self) -> None:
        """
        Inputs new password
        """
        while True:
            new_password = getpass.getpass(ENTER_YOUR_PASSWORD)
            if new_password == '':
                self.password = ''
                break
            regex = re.compile(SPECIAL_CHARS)
            if len(new_password) < 8:
                print(PASS_MIN_8_CHARS)
            elif new_password.islower() or new_password.isupper():
                print(PASS_MIXED_COUNT)
            elif not(True in [char.isdigit() for char in new_password]):
                print(PASS_ATLEAST_1_DIGIT)
            elif regex.search(new_password) is None:
                print(PASS_ATLEAST_1_SPECIAL_CHAR)
            else:
                confirm_password = input(CONFIRM_YOUR_PASSWORD)
                if confirm_password == new_password:
                    self.password = new_password
                    break
                else:
                    print(CONFIRMATION_PASSWORD_DIDNT_MATCH)
    
    def input_new_database(self) -> None:
        """
        Inputs new database name.
        """
        database = input(ENTER_DATABASE_NAME).strip()
        self.database = database

    def input_table(self) -> None:
        """
        Inputs table name
        """
        table_name = input(ENTER_TABLE_NAME).strip()
        self.table = table_name
    
    def input_new_table(self) -> None:
        """
        Inputs new table name.
        """
        table_name = input(ENTER_TABLE_NAME).strip()
        self.table = table_name
    
    def get_no_of_columns_in_a_table(self) -> None: 
        """
        Retrive no of columns in a table.
        """
        self.cursor.execute(f"SELECT count(*) AS NUMBEROFCOLUMNS FROM \
            information_schema.columns\
            WHERE table_name='{self.table}'")
        self.no_of_columns_in_a_table = self.cursor.fetchall()[0][0]
    
    def execute_a_table_or_record_query_and_return_output(self, query):
        """
        Executes a table or record related query and prints output.
        """
        self.cursor.execute(query)
        try:     
            output = self.cursor.fetchall()
        except mysql.connector.errors.InterfaceError:
            pass
        else:
            return output

if __name__ == MAIN:
    # creating superuser's connection and cursor for postgreSQL
    pg_super_connection = psycopg2.connect(user='postgres',
                                           password='postgres',
                                           host='localhost')
    pg_super_connection.set_session(autocommit=True)
    pg_super_cursor = pg_super_connection.cursor()

    # creating superuser's connection and cursor for MySQL
    my_super_connection = mysql.connector.connect(user='root',
                                                  password='Welcome 2022!',
                                                  host='localhost')
    my_super_cursor = my_super_connection.cursor(buffered=True)
    my_super_connection.autocommit = True

    DbOp()
