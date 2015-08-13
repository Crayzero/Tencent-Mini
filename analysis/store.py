#!/usr/bin/env python
# encoding: utf-8

import redis
import json
import os
import hashlib
import codecs

class RedisStorage:
    def __init__(self, host='localhost', port=6379, db=0):
        try:
            self.pool = redis.ConnectionPool(host=host, port=port, db=db)
        except redis.exceptions.ConnectionError as err:
            print("cann't connect to the server")
            print(err)
            raise(err)

    def _store(self, key, value):
        try:
            r = redis.Redis(connection_pool=self.pool)
            r.set(key, value)
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
        value = None

    def get_top(self, key):
        try:
            s = self.r.get(key).decode()
            obj = json.loads(s)
            s = None
            return obj
        except Exception as e:
            print(e)
            raise(e)
