import json
import re

def format_command(query): 
    """This function takes an SQL query input from the user and tokenzies it

    Args:
        query (string): The users query input

    Returns:
        command: a list containing the tokenized query
    """
    
    command = (list(filter(None,re.split(', |\s|;+|\((.+)\)', query))))


    return command

def process_query(command):
    match command[0].upper():
        case 'CREATE':
            # format_json(format_create_query(command))
            return format_create_query(command)
        case 'DROP':
            # format_json(format_drop_query(command))
            return format_drop_query(command)
        case 'USE':
            # format_json(format_use_query(command))
            return format_use_query(command)
        case 'ALTER':
            # format_json(format_alter_query(command))
            return format_alter_query(command)
        case 'SELECT':
            format_json(format_select_query(command))
            return format_select_query(command)
        case 'INSERT':
            #return format_insert_query(command)
            return format_insert_query(command)
        case 'UPDATE':
            return format_update_query(command)
        case 'DELETE':
            return format_delete_query(command)
        case '.EXIT':
            # format_json({"type": "EXIT"})
            return {"type": "EXIT"}
        case _:
            print("Invalid SQL query")
            return None

def format_json(query, file_name = 'data/query_list.json'):
    """This functions writes out SQL queries to a JSON file

    Args:
        commands (list): a list of tokenized SQL queries, each index representing one query
        file_name (.json): A JSON file that will store each SQL query
    """
    #is the command a valid SQL query?
    with open(file_name, "r") as json_file:
        query_json = json.load(json_file)

    query_json['Queries'].append(query)
        
    with open(file_name, "w") as f:
        json.dump(query_json, f, indent=4)
    
def format_create_query(token_list): 
    """This function formats a SQL CREATE query into a JSON file

    Args:
        token_list (list): a list with a tokenized SQL query

    Returns:
        data: a dictionary with all the information about a CREATE DATABASE or CREATE TABLE query
    """
    if token_list[1] == 'DATABASE':
        with open("data/query formats/create_query.json", "r") as f: #open default create json to format new input
            data = json.load(f)

        data['request'] = 'DATABASE'
        data['format']['DATABASE']['name'] = token_list[2]

        return data

    elif token_list[1] == 'TABLE':
        with open("data/query formats/create_query.json", "r") as f:
            data = json.load(f)

        data['request'] = 'TABLE'
        data['format']['TABLE']['name'] = token_list[2]
        variable_list = list(filter(None,re.split(', |\s|;+', token_list[3])))
        
        var_index = 0
        while var_index < len(variable_list): #add all the variables to the query
            data['format']['TABLE']['variables'].append({"name": variable_list[var_index],
                                                                "datatype": variable_list[var_index + 1] })
            var_index += 2

        return data

def format_drop_query(token_list):
    if token_list[1] == 'DATABASE':
        with open("data/query formats/drop_query.json", "r") as f: #open default create json to format new input
            data = json.load(f)
        
        data['request'] = 'DATABASE'
        data['name'] = token_list[2]

        return data
    
    elif token_list[1] == 'TABLE':
        with open("data/query formats/drop_query.json", "r") as f: #open default create json to format new input
            data = json.load(f)

        data['request'] = 'TABLE'
        data['name'] = token_list[2]

        return data

def format_use_query(token_list):
    with open("data/query formats/use_query.json", "r") as f: #open default create json to format new input
        data = json.load(f)
    
    data['name'] = token_list[1]

    return data

def format_alter_query(token_list):
    with open("data/query formats/alter_query.json", "r") as f:
        data = json.load(f)

    data['request'] = 'TABLE'
    data['format']['ADD']['name'] = token_list[2]
    variable_list = token_list[4:] #new list with only variable names

    var_index = 0
    while var_index < len(variable_list): #add all the variables to the query
        data['format']['ADD']['variables'].append({"name": variable_list[var_index],
                                                            "datatype": variable_list[var_index + 1] })
        var_index += 2

    return data

def format_select_query(token_list):
    #use list comprehension to lower() all things. take the elemenet after from which is the table name
    temp = [x.upper() for x in token_list] #check against this, to not worry about being case-sensitive for query
    with open("data/query formats/select_query.json", "r") as f:
        data = json.load(f)
    
    from_index = temp.index('FROM')
    
    if 'WHERE' in temp:
        where_index = temp.index('WHERE')
        data['where']['attribute'] = token_list[where_index + 1] #store the column name we are matching against
        data['where']['operator'] = token_list[where_index + 2] #store the operator we are checking values with
        data['where']['value'] = token_list[where_index + 3]
        if (where_index - from_index) > 2: #join query (not using join statement), so store the aliases for the table names
            data['tables'] = token_list[from_index + 1:where_index:2]
            data['aliasList'] = token_list[from_index + 2:where_index:2]#aliases are two indexes apart so jump by 2 indexes
            data['isJoin'] = True
    elif 'ON' in temp:
        on_index = temp.index('ON')
        data['isJoin'] = True
        data['on']['attribute'] = token_list[on_index + 1] #store the attribute we are matching against
        data['on']['operator'] = token_list[on_index + 2] #store the operator we are evaluating with
        data['on']['value'] = token_list[on_index + 3] #store the second attribute we are matching against
        if 'INNER' in temp: #check type of join we're performing
            data['joinType']['isInner'] = True
            data['tables'] = token_list[from_index + 1:on_index:4] 
            data['aliasList'] = token_list[from_index + 2:on_index:4] #store table aliases
        elif 'LEFT' in temp:
            data['joinType']['isLeft'] = True
            data['tables'] = token_list[from_index + 1:on_index:5] 
            data['aliasList'] = token_list[from_index + 2:on_index:5] #store table aliases

    if token_list[1] == '*':
        data['allColumns'] = True
    else:
        data['allColumns'] = False
        column_list = token_list[1:from_index] # store all the values  
        data['columns'] = column_list
    if not data['isJoin']:#if the select isn't joining tables then there is only one table selected
        data['tableName'] = token_list[from_index + 1] #the word after FROM is the table to select from

    return data

def format_insert_query(token_list): 
    value_list = list(filter(None,re.split(',\s|\s|;+', token_list[4]))) #4th index holds all the values, so tokenize each one.
    with open("data/query formats/insert_query.json", "r") as f: #open default insert json to format new input
        data = json.load(f)
    data['tableName'] = token_list[2]

    for index, value in enumerate(value_list):#add the values that the user wants to insert into a list
        data['variableValues'].append(value_list[index])
    return data

def format_delete_query(token_list):
    with open("data/query formats/delete_query.json", "r") as f: #open default insert json to format new input
        data = json.load(f)
    data['tableName'] = token_list[2]

    where_index = 3

    data['where']['attribute'] = token_list[where_index + 1] #store the column name we are matching against
    data['where']['operator'] = token_list[where_index + 2] #store the operator we are checking values with
    data['where']['value'] = token_list[where_index + 3]

    return data


def format_update_query(token_list):
    temp = [x.upper() for x in token_list] #check against this, to not worry about being case-sensitive for query
    with open("data/query formats/update_query.json", "r") as f: #open default insert json to format new input
        data = json.load(f)
    data['tableName'] = token_list[1]
    
    start = 3 #the start of the list columns we are setting a value
    end = temp.index('WHERE') #the last column we are setting values for will appear one index before where

    set_list = token_list[start:end]

    for i in range(0, len(set_list), 3): #format the set part of the query
        data['set'].append({"attribute": set_list[i],
                                "operator": set_list[i + 1],
                                "value": set_list[i + 2] })

    data['where']['attribute'] = token_list[end + 1] #store the column name we are matching against
    data['where']['operator'] = token_list[end + 2] #store the operator we are checking values with
    data['where']['value'] = token_list[end + 3]

    return data


def reset_query_list(): #resets the query list json file to a dictionary with an empty list
    with open("data/query_list.json", "w") as json_file:
        query_json = json.load(json_file)
    query_json = { "Queries": [] }
    with open("data/query_list.json", "w") as f:
        json.dump(query_json, f, indent=4)
