#!/usr/bin/env python
# encoding: utf-8

import config
import mysql.connector
from mysql.connector import errorcode

def op_tables(statement):
    try:
        cnx = mysql.connector.connect(**config.mysql)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERR:
            print("bad database error")
        else:
            print(err)
        raise(err)
    try:
        cursor = cnx.cursor()
        for i in config.prov_to_table:
            print(statement.format(config.prov_to_table[i]))
            cursor.execute(statement.format(config.prov_to_table[i]))
    except mysql.connector.Error as err:
        print(err)
        raise(err)

def build_tables():
    create_statement = "CREATE TABLE IF NOT EXISTS {} LIKE logs"
    op_tables(create_statement)


def drop_table():
    drop_statement = "DROP TABLE {}"
    op_tables(drop_statement)

def clear_table():
    clear_statement = "DELETE FROM {}"
    op_tables(clear_statement)


if __name__ == '__main__':
    import sys
    if sys.argv[1] == 'build':
        build_tables()
    elif sys.argv[1] == 'drop':
        drop_table()
    elif sys.argv[1] == 'clear':
        clear_table()
