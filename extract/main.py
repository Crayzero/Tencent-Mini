#!/usr/bin/env python
# encoding: utf-8

import config
import codecs
import mysql.connector
from mysql.connector import errorcode
import socket
import ip_info
import re
#from multiprocessing import Pool, Process, Queue
#from multiprocessing import Value, Lock
import threading
import time
import sys
sys.path.append('../analysis/')
import analysis
import cProfile

cityService = None
pattern = None
ip_pattern = None
statistic = None


class Extract:
    def __init__(self):
        self.__file = config.log_file
        self.dns = {}

    def extract(self):
        with codecs.open(self.__file, "r", 'cp936') as f:
            self.line_count = 0
            self.lines = f.readlines()
            print("finished read file")
            return self.process_line()

    #@profile
    def process_line(self):
        statistic = analysis.Statistics()
        cityService = ip_info.CityService()
        pattern = re.compile('https?://((\w+\.?)+)/.*')
        ip_pattern = re.compile('^(d{1,2}|1dd|2[0-4]d|25[0-5]).(d{1,2}|1dd|2[0-4]d|25[0-5]).(d{1,2}|1dd|2[0-4]d|25[0-5]).(d{1,2}|1dd|2[0-4]d|25[0-5])$')

        res = []
        count = 0
        for line in self.lines:
            count += 1
            if count >= 100000:
                pass
                #break
            columns = line.split("\t")
            datetime = columns[1]
            explain = columns[2]
            src_ip = columns[3]
            name = columns[4].split('.')[0]
            isp = columns[5]
            prov = columns[6]
            city = cityService.get_city_by_ip(src_ip)
            if city == None:
                city = 'NULL'
            addr = columns[7]
            addr_url = pattern.match(addr).group(1)
            if ip_pattern.match(addr_url):
                source_addr = cityService.get_city_by_ip(addr_url)
            else:
                if self.dns.get(addr_url):
                    source_addr = self.dns[addr_url]
                else:
                    source_addr = socket.getaddrinfo(addr_url, 80)[0][-1][0]
                    source_addr = cityService.get_city_by_ip(source_addr)
                    self.dns[addr_url] = source_addr
            extra = columns[8]
            extra = extra.strip()
            l = (datetime, explain, src_ip, name, isp, prov, city, addr_url, source_addr, extra)
            res.append(l)
            statistic.count(l)
            l = None
            datetime = None
            explain = None
            src_ip = None
            name = None
            isp = None
            prov = None
            city = None
            addr_url = None
            source_addr = None
            extra = None
            line = None
        self.lines = None
        pattern = None
        ip_pattern = None
        cityService.destory()
        statistic.get_top()
        return res

#@profile
def insert_log(cnxpool, log):
    while True:
        try:
            #conn = cnxpool.get_connection()
            conn = mysql.connector.connect(**config.mysql)
            cursor = conn.cursor()
            insert_statement = ' '.join(log)
            #cursor.execute(insert_statement)
            conn.commit()
        except mysql.connector.errors.PoolError:
            pass
        except mysql.connector.errors.DatabaseError as err:
            print(err)
            time.sleep(1)
        except mysql.connector.Error as err:
            print(err)
            conn.rollback()
        else:
            cursor.close()
            conn.close()
            break

#@profile
def store(logs):
    try:
        cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name="pool",
                     pool_size = 2, **config.mysql)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("cann't connect to mysql server")
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            print("database doesn't exist")

    base_insert_log = (u"INSERT INTO {}(datetime,some,IP,name,ISP,prov,city,url,server,extra) VALUES ")
    values = "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"
    insert_state_map = {}
    count_map = {}
    threads = []

    for l in logs:
        prov = l[5]
        insert_state = ''
        if insert_state_map.get(prov) and len(insert_state_map[prov]):
            insert_state = insert_state_map[prov]
            insert_state.append(',')
        else:
            insert_state_map[prov] = []
            insert_state = base_insert_log.format(config.prov_to_table[prov])
            insert_state_map[prov].append(insert_state)
            insert_state = insert_state_map[prov]
        insert_state.append(values % l)
        if count_map.get(prov) == None:
            count_map[prov] = 0
        count_map[prov] += 1
        if count_map[prov] < 2000:
            continue
        else:
            t = threading.Thread(target = insert_log, args=(cnxpool, insert_state[:]), daemon=False)
            #t = Process(target=insert_log, args=(cnxpool, insert_state[:]))
            threads.append(t)
            t.start()
            insert_state_map.pop(prov)
            count_map[prov] = 0
    #cursor.execute("START TRANSACTION")
    for k in insert_state_map:
        t = threading.Thread(target = insert_log, args=(cnxpool, insert_state_map[k][:]), daemon=False)
        #t = Process(target=insert_log, args=(cnxpool, insert_state_map[k][:]))
        t.start()
        threads.append(t)
    insert_state_map = []
    for i in threads:
        i.join()
    #cursor.execute("commit")


def store_detail(details):
    sql_file = open("/home/cz/Documents/mysql/ip_info_insert.sql", 'a+')
    insert_detail = ("INSERT INTO ip_info (ip, address_detail, video_name, video_type, video_png) VALUES (%d, \"%s\", \"%s\", \"%s\", \"%s\");\n")
    for i in details:
        i.replace("\"", "'")
    sql_file.write(insert_detail %(details))
    return 0

    try:
        conn = mysql.connector.connect(**config.mysql)
        cursor = conn.cursor()
        insert_detail = ("INSERT INTO ip_info (ip, address_detail, video_name, video_type, video_png) VALUES (%d, '%s', '%s', '%s', '%s');\n")
        print(details)
        if isinstance(details, tuple):
            details = (details,)
        for d in details:
            print(insert_detail % (d))
            cursor.execute(insert_detail % (d))
            conn.commit()
    except mysql.connector.Error as err:
        print(err)
        conn.rollback()
    cursor.close()
    conn.close()


def get_detail(start=0):
    try:
        conn = mysql.connector.connect(**config.mysql)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("cann't connect to mysql server")
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            print("database doesn't exist")
    cursor = conn.cursor()
    #cursor.execute("set NAMES latin1")

    try:
        if start == 0:
            cursor.execute("select * from logs")
        else:
            cursor.execute("select * from logs limit " + str(start) + ", 10000")
    except Exception as e:
        print(e)
    for i in cursor:
        ip = i[4]
        address = ip_info.get_ip_info(i[4])[-1]
        vid = i[5].split('.')[0]
        video_info = ip_info.get_vedio_info(vid)
        if video_info:
            video_name = video_info[0]
            video_type = video_info[1]
            video_png = video_info[2]
            detail = (vid, ip, str(address), video_name, video_type, video_png)
            yield detail
            #details.append(detail)
    cursor.close()


def get_logs(start=0, limit=0):
    try:
        conn = mysql.connector.connect(**config.mysql)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("cann't connect to mysql server")
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            print("database doesn't exist")
    cursor = conn.cursor()

    try:
        if limit != 0:
            cursor.execute("select * from logs limit " + str(start) + "," + str(limit))
        else:
            cursor.execute("select * from logs limit " + str(start) + ", 18446744073709551615")
    except Exception as e:
        print(e)
    for i in cursor:
        yield i
    cursor.close()

def f(logs):
    for i in logs:
        print(i)

#@profile
def __main():
    e = Extract()
    #cProfile.run('e.extract()', sort='cumulative')
    logs = e.extract()
    print("extract finished")
    #print("logs length is ", len(logs))
    #cProfile.run('store(logs)', sort='cumulative')
    #store(logs)
    logs = None


if __name__ == "__main__":
    __main()
    '''
    e = Extract()
    import cProfile
    cProfile.run('e.extract()', sort='cumulative')
    '''
