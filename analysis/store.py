#!/usr/bin/env python
# encoding: utf-8

import redis
import json
import os
import hashlib
import codecs
import sys
sys.path.append("../extract/")
import config

class RedisStorage:
    def __init__(self, host='localhost', port=6379, db=0):
        self.host = host
        self.port = port
        self.db = db
        self.pool = None
        self.r = None
        self._connect()

    def _connect(self):
        for redis_server in config.redis_servers:
            try:
                self.pool = redis.ConnectionPool(host=self.host, port=self.port, db=self.db)
                self._getconnection()
                return
            except redis.exceptions.ConnectionError as err:
                print("cann't connect to redis on ", self.localhost, " port:", self.port)
                raise(err)
        else:
            print("cann't connect to redis")
            raise(redis.exceptions.ConnectionError("connect"))

    def _getconnection(self):
        for i in config.redis_servers:
            try:
                self.r = redis.Redis(connection_pool=self.pool)
                return
            except redis.exceptions.ConnectionError :
                self._connect()
        else:
            print("can't get connection to redis server")
            raise(redis.exceptions.ConnectionError("get connection"))

    def _store(self, key, value):
        try:
            r = redis.Redis(connection_pool=self.pool)
            r.set(key, value)
        except redis.exceptions.ConnectionError:
            self._getconnection()
        except Exception as e:
            print(e)
            raise(e)

    def file_hash(self, file_path):
        try:
            file_size = os.stat(file_path).st_size
            f = codecs.open(file_path, 'r', 'cp936')
            s = hashlib.sha1()
            data = f.read()
            hash_prefix = 'blob %u\0' % file_size
            s.update(hash_prefix.encode('utf-8'))
            for data in f:
                data = data.encode('utf-8')
                s.update(data)
            data = None
            f.close()
            key = s.hexdigest()
            print("key is ", key)
            return key
        except os.error as e:
            print(e)
            raise(e)
        except Exception as e:
            print(e)
            raise(e)

    def get_file_name(self, file_path):
        return os.path.basename(file_path)

    def store_top(self, file_name, res):
        value = json.dumps(res)
        #self._store(self.file_hash(file_name), value)
        self._store(self.get_file_name(file_name), value)

    def store_part_length(self, file_name, length):
        value = {}
        value['length'] = length
        value['processed'] = {}
        self._store(file_name, json.dumps(value))

    def store(self, key, value, timeout=None):
        self._store(key, value)
        if timeout:
            self.r.expire(key, timeout)

    def get_top(self, key):
        try:
            s = self.r.get(key).decode('utf-8')
            obj = json.loads(s)
            s = None
            return obj
        except Exception as e:
            print(e)
            raise(e)

    def get(self, key):
        try:
            return self.r.get(key)
        except Exception as e:
            print(e)
            raise(e)

    def get_map_result(self, key):
        while True:
            try:
                if self.r == None:
                    self._getconnection()
                s = self.r.get(key)
                if s != None:
                    s = s.decode()
                else:
                    return s
                #print(key + "'s size is ", len(s))
                obj = json.loads(s)
                s = None
                return obj
            except redis.exceptions.ConnectionError as e:
                self._getconnection()
            except Exception as e:
                print(e)
                raise(e)

    def delete(self, key):
        while True:
            try:
                self.r.delete(key)
                return
            except redis.exceptions.ConnectionError:
                self._getconnection()
