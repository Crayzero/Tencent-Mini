#!/usr/bin/env python
# encoding: utf-8

import config
import mysql.connector
from mysql.connector import errorcode

def op_tables(statement):
    res = []
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
            try:
                cursor.execute(statement.format(config.prov_to_table[i]))
                result = cursor.fetchone()
                if result:
                    for i in result:
                        res.append(i)
                cnx.commit()
            except mysql.connector.errors.ProgrammingError as err:
                print(err)
    except mysql.connector.Error as err:
        print(err)
        raise(err)
    return res

def build_tables():
    create_statement = "CREATE TABLE IF NOT EXISTS {} LIKE logs"
    op_tables(create_statement)


def drop_table():
    drop_statement = "DROP TABLE {}"
    op_tables(drop_statement)

def clear_table():
    clear_statement = "DELETE FROM {}"
    op_tables(clear_statement)

def get_total():
    total_statement = "SELECT COUNT(*) FROM {}"
    res = op_tables(total_statement)
    cnt = 0
    for i in res:
        cnt += i
    print("total is ", cnt)


if __name__ == '__main__':
    import sys
    if sys.argv[1] == 'build':
        build_tables()
    elif sys.argv[1] == 'drop':
        drop_table()
    elif sys.argv[1] == 'clear':
        clear_table()
    elif sys.argv[1] == 'total':
        get_total()
