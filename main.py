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

def match_where(column, operator, value):
    """This function evalutaes a where query on a tuple in a table

    Args:
        column (string, float, int): represents the value of a column in a tuple
        operator (string): represents the operator in a where query (e.g >, <)
        value (int, string, float): the value the where query is matching against

    Returns:
        boolean: based on evaluation of where query
    """
    match operator:
        case '=':
            return True if column == value else False
        case '!=':
            return True if column != value else False
        case '>':
            return True if column > value else False
        case '<':
            return True if column < value else False
        case '>=':
            return True if column >= value else False
        case '<=':
            return True if column <= value else False

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
                            records = list(csv_reader)
                            with open(curr_database + "/" + table_name + ".json", 'r') as json_table_file: #open table's json file to view field's datatypes
                                datatype_list = json.load(json_table_file) #load the field's corresponding datatypes into a list
                                for i in range(len(records[0])):
                                    print(f"{records[0][i]} {datatype_list[i]['datatype']}", end='')
                                    if i != (len(records[0]) - 1):
                                        print(' | ',end='')
                                print()
                                for line in records[1:]: #each line in the csv_reader is a list, so print the attributes values
                                    print(" | ".join(line)) #join each element in line with ' | '
                    else: #if we are selecting specific columns
                        columns = query['columns']
                        index_columns = [] #this will store the indexes of the columns (based on the csv) the user has selected
                        if query['where']['attribute'] == None: #if select query doesn't use where clause
                            with open(curr_database + "/" + table_name + ".csv", 'r') as csv_table_file:
                                csv_reader = csv.reader(csv_table_file)
                                records = list(csv_reader)
                                with open(curr_database + "/" + table_name + ".json", 'r') as json_table_file: #open table's json file to view field's datatypes
                                    datatype_list = json.load(json_table_file) #load the field's corresponding datatypes into a list
                                    for i, column_name in enumerate(columns):
                                        field_index = records[0].index(column_name) #holds the index where a specific column name appears in the csv
                                        index_columns.append(field_index)
                                        print(f"{column_name} {datatype_list[field_index]['datatype']}", end='')
                                        if i != (len(columns) - 1):
                                            print(' | ',end='')
                                    print() #print newlien for formatting
                                    for row in records[1:]: #loop through each record in the csv
                                        for i in range(len(columns)): #loop through each attribute value in the record and only print the columns the user requested
                                            print(row[index_columns[i]], end='')
                                            if i != (len(columns) - 1): #only print the bar if it isnt the last element
                                                print(' | ',end='')
                                        print()
                        else:
                            operator = query['where']['operator'] #store the operator the where clause is matching against
                            where_column = query['where']['attribute'] #store the column name 
                            where_value = query['where']['value'] #store the value in the where cluase
                            table_types = [] #will hold the datatypes for each column in the table
                            with open(curr_database + "/" + table_name + ".csv", 'r') as csv_table_file:
                                csv_reader = csv.reader(csv_table_file)
                                records = list(csv_reader)
                                with open(curr_database + "/" + table_name + ".json", 'r') as json_table_file: #open table's json file to view field's datatypes
                                    datatype_list = json.load(json_table_file) #load the field's corresponding datatypes into a list
                                    for entry in datatype_list: #store the datatypes of each column in a list
                                        if entry['datatype'] == 'int':
                                            table_types.append(int)
                                        elif entry['datatype'] == 'varchar(20)':
                                            table_types.append(str)
                                        elif entry['datatype'] == 'float':
                                            table_types.append(float)
                                    for i, column_name in enumerate(columns):
                                        field_index = records[0].index(column_name) #holds the index where a specific column name appears in the csv
                                        index_columns.append(field_index)
                                        print(f"{column_name} {datatype_list[field_index]['datatype']}", end='')
                                        if i != (len(columns) - 1):
                                            print(' | ',end='')
                                    print() #print newlien for formatting
                                    match_column = records[0].index(where_column) #check where clause against specific column
                                    column_type = table_types[match_column]

                                    row = 1
                                    while row <= len(records[1:]): #print out the records that match the select query
                                        column_value = records[row][match_column]
                                        if column_type == int: #python converts csv values to strings so we need to typecast values based on how the table is structured
                                            column_value = int(column_value)
                                            where_value = float(where_value)
                                        elif column_type == float:
                                            column_value = float(column_value)
                                            where_value = float(where_value)
                                        elif column_type == str:
                                            pass #nothing needs to be done
                                        if match_where(column_value, operator, where_value):
                                            for i in range(len(columns)): #loop through each attribute value in the record and only print the columns the user requested
                                                print(records[row][index_columns[i]], end='')
                                                if i != (len(columns) - 1): #only print the bar if it isnt the last element
                                                    print(' | ',end='')
                                            print()
                                        row += 1
        case 'ALTER TABLE':
            table_name = query['format']['ADD']['name']
            if curr_database == None:
                print("!Failed to alter table, no database selected.")
            else:
                if not os.path.isfile(curr_database + "/" + table_name + ".csv"): #does the table exist?
                    print(f"!Failed to alter table {table_name} because it does not exist.")
                else: #adding more fields to table
                    with open(curr_database + "/" + table_name + ".json", 'r') as json_table_file:
                        datatype_list = json.load(json_table_file)
                    with open(curr_database + "/" + table_name + ".csv", 'r') as csv_table_file:
                        csv_reader = csv.reader(csv_table_file)
                        rows = list(csv_reader) 
                        rows[0].append(query['format']['ADD']['variables'][0]['name']) #add the new variable to the end of the field list
                    with open(curr_database + "/" + table_name + ".csv", 'w') as csv_table_file:
                        csv_writer = csv.writer(csv_table_file)
                        csv_writer.writerows(rows) #write back all the data to the csv.
                    with open(curr_database + "/" + table_name + ".json", 'w') as json_table_file:
                        datatype_list.append({"datatype": query['format']['ADD']['variables'][0]['datatype']})
                        json.dump(datatype_list, json_table_file, indent=4)
                    print(f"Table {table_name} modified.")
        case 'INSERT':
            table_name = query['tableName']
            if curr_database == None:
                print("!Failed to insert into table, no database selected.")
            else:
                if not os.path.isfile(curr_database + "/" + table_name + ".csv"): #does the table exist?
                    print(f"!Failed to insert into table {table_name} because it does not exist.")
                else:
                    with open(curr_database + "/" + table_name + ".csv", 'a', newline='') as csv_table_file: #append values to end of file
                        csv_writer = csv.writer(csv_table_file)
                        csv_writer.writerow(query['variableValues'])
                    print("1 new record inserted.")
        case 'UPDATE':
            table_name = query['tableName']
            where_column = query['where']['attribute'] #store the column name 
            where_value = query['where']['value'] #store the value in the where cluase
            operator = query['where']['operator']
            table_types = [] #will hold the datatypes for each column in the table
            if curr_database == None:
                print("!Failed to update table, no database selected.")
            else:
                if not os.path.isfile(curr_database + "/" + table_name + ".csv"): #does the table exist?
                    print(f"!Failed to insert into table {table_name} because it does not exist.")
                else:
                    with open(curr_database + "/" + table_name + ".json", 'r') as json_table_file:
                        datatype_list = json.load(json_table_file)
                    for entry in datatype_list:#store the datatypes of each column in a list
                        if entry['datatype'] == 'int':
                            table_types.append(int)
                        elif entry['datatype'] == 'varchar(20)':
                            table_types.append(str)
                        elif entry['datatype'] == 'float':
                            table_types.append(float)
                    with open(curr_database + "/" + table_name + ".csv", 'r') as csv_table_file:
                        csv_reader = csv.reader(csv_table_file)
                        records = list(csv_reader)
                    with open(curr_database + "/" + table_name + ".csv", 'w') as csv_table_file:
                        csv_writer = csv.writer(csv_table_file)
                        match_column = records[0].index(where_column) #check where clause against specific column
                        column_type = table_types[match_column] #holds the datatype of the column we are checking against
                        records_updated = 0

                        row = 1
                        while row <= len(records[1:]):
                            column_value = records[row][match_column] #type cast the column value
                            if column_type == int:
                                column_value = int(column_value)
                                where_value = float(where_value)
                            elif column_type == float:
                                column_value = float(column_value)
                                where_value = float(where_value)
                            elif column_type == str:
                                pass #nothing needs to be done
                            if match_where(column_value, operator, where_value): #does the record match the where clause query? Skip row 0 (headers)
                                for i in range(len(query['set'])): #loop through all the columns user wants to change
                                    column = records[0].index(query['set'][i]['attribute']) #find the index of the column the user wants to update 
                                    records[row][column] = query['set'][i]['value'] #update records to reflect the change to the specific column value 
                                records_updated += 1
                            row += 1
                        print(f"{records_updated} records modified") if records_updated > 1 else print(f"{records_updated} record modified")
                        csv_writer.writerows(records) #write back the update values to the csv
        case 'DELETE':
            table_name = query['tableName']
            where_column = query['where']['attribute'] #store the column name 
            where_value = query['where']['value'] #store the value in the where cluase
            operator = query['where']['operator']
            table_types = [] #will hold the datatypes for each column in the table
            if curr_database == None:
                print("!Failed to delete record, no database selected.")
            else:
                if not os.path.isfile(curr_database + "/" + table_name + ".csv"): #does the table exist?
                    print(f"!Failed to delete record from table {table_name} because it does not exist.")
                else:
                    with open(curr_database + "/" + table_name + ".json", 'r') as json_table_file:
                        datatype_list = json.load(json_table_file)
                    for entry in datatype_list:#store the datatypes of each column in a list
                        if entry['datatype'] == 'int':
                            table_types.append(int)
                        elif entry['datatype'] == 'varchar(20)':
                            table_types.append(str)
                        elif entry['datatype'] == 'float':
                            table_types.append(float)
                    with open(curr_database + "/" + table_name + ".csv", 'r') as csv_table_file:
                        csv_reader = csv.reader(csv_table_file)
                        records = list(csv_reader)
                    with open(curr_database + "/" + table_name + ".csv", 'w') as csv_table_file:
                        csv_writer = csv.writer(csv_table_file)
                        match_column = records[0].index(where_column) #check where clause against specific column
                        column_type = table_types[match_column] #holds the datatype of the column we are checking against
                        records_deleted = 0
                        
                        row = 1
                        while row <= len(records[1:]):
                            column_value = records[row][match_column] #type cast the column value
                            if column_type == int:
                                column_value = int(column_value)
                                where_value = float(where_value)
                            elif column_type == float:
                                column_value = float(column_value)
                                where_value = float(where_value)
                            elif column_type == str:
                                pass #nothing needs to be done
                            if match_where(column_value, operator, where_value): #does the record match the where clause query? Skip row 0 (headers)
                                del records[row] #remove the record entry from the table
                                row -= 1 #decrement row count so we don't end up an index ahead from where we should be
                                records_deleted += 1
                            row += 1
                        print(f"{records_deleted} records deleted") if records_deleted > 1 else print(f"{records_deleted} record deleted")
                        csv_writer.writerows(records) #write back the new table 
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