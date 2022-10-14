"""
    Author : Kaiser Sakhi
    Date : 13-10-2022
"""

"""

    Program : To convert a json array into sqlite database

"""

import sys
import os
import json
import sqlite3
import urllib.parse
import urllib


def does_file_exist(path):
    return os.path.isfile(path=path)


def parse_json_and_store(file_path, db_conn, col_list):
    col_num = len(col_list)
    file = open(file=file_path)
    data = json.load(file)
    insert_query = "INSERT INTO json_data values("
    for item in data:
        temp_query = insert_query
        # for x in item:
        for col_item_index in range(col_num):
            temp_query = temp_query + "'" + urllib.parse.quote_plus(str(item[col_list[col_item_index]].strip())) + "',"
        temp_query = temp_query[0:len(temp_query) - 1] + ");"
        print(temp_query)
        db_conn.execute(temp_query)
    db_conn.commit()
    db_conn.close()


def get_file_name(file_path):
    name = ""
    is_extension_over = False
    for i in range(len(file_path) - 1, -1, -1):
        if file_path[i] == '.' and not is_extension_over:
            is_extension_over = True
            continue
        if is_extension_over and (file_path[i] != '/'):
            name += file_path[i]
        elif is_extension_over:
            break

    return name[::-1]


def create_database(column_names, file_name):
    # col_len = len(column_names)
    conn = sqlite3.connect("./"+file_name.strip() + ".db")
    create_query = "CREATE TABLE IF NOT EXISTS json_data ("

    for x in column_names:
        create_query += x + ","

    create_query = create_query[0:len(create_query) - 1]

    create_query += ");"

    conn.execute(create_query)
    print(create_query)
    return conn


def extract_column_names(column_fields):
    names = []
    for x in column_fields:
        x = x.strip()
        if len(x) > 0:
            names.append(x.partition(" ")[0])
    return names


def main():
    if len("sys.argv") < 3:
        print("From Json To Sqlite Database converter\n")
        print("Please pass file path and column names with data types.")
        print("Example : main.py 'data.json' 'name TEXT,id INTEGER PRIMARY KEY'")
        exit(0)
    json_file_path = sys.argv[1]
    # json_file_path = "data.json"
    column_fields = sys.argv[2].split(",")
    # column_fields = 'text TEXT, author TEXT'.split(",")

    for i in range(len(column_fields)):
        column_fields[i] = column_fields[i].strip()

    db_conn = create_database(column_fields, get_file_name(json_file_path))

    column_names = extract_column_names(column_fields)

    # print(column_names)

    if not does_file_exist(json_file_path):
        print("File doesn't exit!")
        exit(0)

    parse_json_and_store(json_file_path, db_conn, column_names)


if __name__ == "__main__":
    main()
