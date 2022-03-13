from operator import is_
import format as form
import csv
import sys
import json
import shutil
import os

# if len(sys.argv) == 2: #pass in the file name as cmd line argument
#     file_name = sys.argv[1]

# file_name = 'PA1_test.sql'

# command_list = form.format_command(file_name)#create command list from SQL file
# form.format_json(command_list,'data/query_list.json') #create list of all queries in json format

curr_database = None #the current database in use

def check_query(query):
    """This function processes SQL queries

    Args:
        query (list): tokenized SQL query

    Returns:
        is_valid: returns true if the query was a valid SQL query and false otherwise
    """
    global curr_database
    match query['type']: 
        case 'CREATE':
            if query['request'] == 'DATABASE': #creating a database
                db_name = query['format']['DATABASE']['name']
                if os.path.isdir(db_name): #check if the database we want to create already exists
                    print(f"!Failed to create database {query['format']['DATABASE']['name']} because it already exists.")
                else:
                    os.mkdir(db_name)
                    print(f"Database {db_name} created.")
            elif query['request'] == 'TABLE': #creating a table
                table_name = query['format']['TABLE']['name']
                if curr_database == None:
                    print("!Failed to create table, no database selected.")
                elif not curr_database == None:
                    if os.path.isfile(curr_database + "/" + table_name + ".csv"):
                        print(f"!Failed to create table {table_name} because it already exists.")
                    else:
                        variable_list = query['format']['TABLE']['variables']
                        json_variable_list_type = []
                        with open(curr_database + "/" + table_name + ".json", 'w') as json_table_file:
                            for i in range(len(variable_list)):
                                json_variable_list_type.append({"datatype": variable_list[i]['datatype']}) #keeps track of the data types for each column(field) of the table
                            json.dump(json_variable_list_type, json_table_file, indent=4)

                        with open(curr_database + "/" + table_name + ".csv", 'w') as csv_table_file: #Create the table as a csv file
                            csv_writer = csv.writer(csv_table_file)
                            fields = []
                            for i in range(len(variable_list)): #loop through variable list in query
                                fields.append(variable_list[i]['name'])
                            csv_writer.writerow(fields)
                        print(f"Table {table_name} created.")
        case 'DROP':
            if query['request'] == 'DATABASE':
                db_name = query['name']
                if not os.path.isdir(db_name):
                    print(f"!Failed to delete {db_name} because it does not exist.")
                else:
                    shutil.rmtree(db_name)
                    print(f"Database {db_name} deleted.")
            elif query['request'] == 'TABLE':
                table_name = query['name']
                if not os.path.isfile(curr_database + "/" + table_name + ".csv"):
                    print(f"!Failed to delete {table_name} because it does not exist.")
                else:
                    os.remove(curr_database + "/" + table_name + ".csv")
                    os.remove(curr_database + "/" + table_name + ".json")
                    print(f"Table {table_name} deleted.")

        case 'USE':
            db_name = query['name']

            if not os.path.isdir(db_name):
                print(f"!Failed to select database {db_name} because it does not exist.")
            else:
                curr_database = db_name
                print(f"Using database {db_name}.")
        case 'SELECT':
            table_name = query['tableName']
            selectAll = query['allColumns']

            if curr_database == None:
                print("!Failed to select columns, no database selected.")
            else:
                if not os.path.isfile(curr_database + "/" + table_name + ".csv"): #does the table exist?
                    print(f"!Failed to query table {table_name} because it does not exist.")
                else:
                    if selectAll:
                        with open(curr_database + "/" + table_name + ".csv", 'r') as csv_table_file:
                            csv_reader = csv.reader(csv_table_file)
                            fields = next(csv_reader) #read in field names and store as a list
                        with open(curr_database + "/" + table_name + ".json", 'r') as json_table_file: #open table's json file to view field's datatypes
                            datatype_list = json.load(json_table_file) #load the field's corresponding datatypes into a list
                            for i in range(len(fields)):
                                if not i == (len(fields) - 1): #this makes sure that a | does not print after the last field
                                    print(f"{fields[i]} {datatype_list[i]['datatype']} | ", end='') #match the field with its datatype
                                else:
                                    print(f"{fields[i]} {datatype_list[i]['datatype']}")
                    # else: #if we are choosing specific columns instead.
        case 'ALTER TABLE':
            table_name = query['format']['ADD']['name']

            if curr_database == None:
                print("!Failed to alter table, no database selected.")
            else:
                if not os.path.isfile(curr_database + "/" + table_name + ".csv"): #does the table exist?
                    print(f"!Failed to alter table {table_name} because it does not exist.")
                else: #adding more fields to table
                    with open(curr_database + "/" + table_name + ".json", 'r') as json_table_file:
                        variable_list = json.load(json_table_file)
                    with open(curr_database + "/" + table_name + ".csv", 'r') as csv_table_file:
                        csv_reader = csv.reader(csv_table_file)
                        rows = list(csv_reader) 
                        rows[0].append(query['format']['ADD']['variables'][0]['name']) #add the new variable to the end of the field list
                    with open(curr_database + "/" + table_name + ".csv", 'w') as csv_table_file:
                        csv_writer = csv.writer(csv_table_file)
                        csv_writer.writerows(rows) #write back all the data to the csv.
                    with open(curr_database + "/" + table_name + ".json", 'w') as json_table_file:
                        variable_list.append({"datatype": query['format']['ADD']['variables'][0]['datatype']})
                        json.dump(variable_list, json_table_file, indent=4)
                    print(f"Table {table_name} modified.")
        case 'INSERT':
            table_name = query['tableName']
            if curr_database == None:
                print("!Failed to insert into table, no database selected.")
            else:
                if not os.path.isfile(curr_database + "/" + table_name + ".csv"): #does the table exist?
                    print(f"!Failed to insert into table {table_name} because it does not exist.")
                else:
                    with open(curr_database + "/" + table_name + ".csv", 'r') as csv_table_file:
                        csv_reader = csv.reader(csv_table_file)
                        rows = list(csv_reader)
                    with open(curr_database + "/" + table_name + ".csv", 'a', newline='') as csv_table_file: #append values to end of file
                        csv_writer = csv.writer(csv_table_file)
                        csv_writer.writerow(query['variableValues'])
        case 'EXIT':
            print("\nprogram termination")

print("Enter SQL query and press enter")
while True:
    user_input = input()
    command = form.format_command(user_input)
    query = form.process_query(command)

    if query == None: #Invalid sql query
        continue
    elif query['type'] == 'EXIT': #program quit
        check_query(query)
        break
    else: #valid sql query, process it accordingly
        check_query(query)