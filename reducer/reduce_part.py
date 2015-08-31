#!/usr/bin/env python
# encoding: utf-8

import pika
import sys
sys.path.append('../analysis/')
import store
sys.path.append('../extract')
import config
import json
import redlock
import time
import random
import heapq

class ReducePart:
    def __init__(self):
        self.lock_time = 5000
        self.dlm = redlock.Redlock(config.redis_servers)
        self.queue = 'part_task_finished'
        self.redis_client = store.RedisStorage()
        self.lock = None

    def run(self):
        cnx = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = cnx.channel()

        channel.queue_declare(queue=self.queue)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(self.callback, queue=self.queue)

        print("waiting for part result")
        channel.start_consuming()

    def callback(self, ch, method, properties, body):
        try:
            msg = body.decode('utf-8')
            msg = json.loads(msg)
            print(msg)

            part_file_name = msg['key']
            name_index = part_file_name.find('_part_')
            if name_index == -1:
                file_name = part_file_name
            else:
                file_name = part_file_name[:name_index]

            while True:
                self.lock = self.dlm.lock("lock_" + file_name, self.lock_time)
                if not self.lock:
                    time.sleep(random.uniform(1,3))
                else:
                    print('acquired lock')
                    break
            print(file_name, part_file_name)
            if file_name != part_file_name:
                self.union(file_name, part_file_name)
            else:
                #file is not splited, so direct report the new result
                self.report_new_result(file_name)
            ch.basic_ack(delivery_tag=method.delivery_tag)

            #ch.basic_cancel('a')
        except Exception as e:
            print(e)
            raise(e)
        finally:
            self.dlm.unlock(self.lock)
            print('unlock the lock')

    def union(self, file_name, part_file_name):
        middle_result_bytes = self.redis_client.get(file_name)
        part_result_bytes = self.redis_client.get(part_file_name)
        middle_result = json.loads(middle_result_bytes.decode('utf-8'))
        part_result = json.loads(part_result_bytes.decode('utf-8'))

        start_time = None
        end_time = None

        if middle_result['processed'].get(part_file_name) is not None:
            print(part_file_name, " has been processed")
            return
        middle_result['processed'][part_file_name] = 1

        if middle_result.get('result') == None:
            middle_result['result'] = part_result
        else:
            for key in part_result:
                if key == 'source':
                    self.union_source(middle_result['result']['source'], part_result['source'])
                elif key == 'time':
                    self.union_time(middle_result['result']['time'], part_result['time'])
                else:
                    self.union_prov(middle_result['result'][key], part_result[key])

        if len(middle_result['processed']) == middle_result['length']:
            print("union all parts")
            (start_time, end_time) = self.union_all(middle_result['result'])
            self.redis_client._store(file_name, json.dumps(middle_result))
            self.report_new_result(file_name, start_time, end_time)
        else:
            self.redis_client._store(file_name, json.dumps(middle_result))

        self.redis_client.delete(part_file_name)


    def union_source(self, middle_source, part_source):
        if middle_source is None:
            middle_source = {}
        for src_city in part_source:
            if middle_source.get(src_city) is None:
                middle_source[src_city] = {}
            for dst_city in part_source[src_city]:
                if middle_source[src_city].get(dst_city) is None:
                    middle_source[src_city][dst_city] = 0
                middle_source[src_city][dst_city] += part_source[src_city][dst_city]


    def union_time(self, middle_time, part_time):
        if middle_time is None:
            middle_time = {}
        for key_time in part_time:
            if middle_time.get(key_time) is None:
                middle_time[key_time] = part_time[key_time]
            else:
                middle_time[key_time] += part_time[key_time]


    def union_prov(self, middle_prov, part_prov):
        if part_prov.get('count') is not None:
            for vid in part_prov['count']:
                if middle_prov.get('count') is None:
                    middle_prov['count'] = {}
                if middle_prov['count'].get(vid) is None:
                    middle_prov['count'][vid] = part_prov['count'][vid]
                else:
                    middle_prov['count'][vid] += part_prov['count'][vid]

        if part_prov.get('city') is not None:
            for city in part_prov['city']:
                if middle_prov.get('city') is None:
                    middle_prov['city'] = {}
                if middle_prov['city'].get(city) is None:
                    middle_prov['city'][city] = {'count': 0}
                if middle_prov['city'][city].get('count') is None:
                    middle_prov['city'][city]['count'] = 0
                middle_prov['city'][city]['count'] += part_prov['city'][city]['count']

        if part_prov.get('total') is not None:
            if middle_prov.get('total') is None:
                middle_prov['total'] = 0
            middle_prov['total'] += part_prov['total']

    def union_all(self, middle_result):
        middle_result['count_per_five_second'] = []
        min_time = None
        max_time = None
        if middle_result.get('time') is not None:
            time_heap = []
            time_heap = sorted(middle_result['time'])
            min_time = time_heap[0]
            max_time = time_heap[-1]
            count = 0
            total = 0
            for key_time in time_heap:
                total += middle_result['time'][key_time]
                count += 1
                if count >= 5:
                    middle_result['count_per_five_second'].append(total)
                    count = 0
                    total = 0
            time_heap = None
            middle_result.pop('time')
        return (min_time, max_time)



    def report_new_result(self, file_name, start_time=None, end_time=None):
        msg = {}
        msg['start_time'] = start_time
        msg['end_time'] = end_time
        msg['key'] = file_name.split('/')[-1]
        value = json.dumps(msg)
        for server in config.rabbitmq_servers:
            try:
                cnx = pika.BlockingConnection(pika.ConnectionParameters(host=server['host']))
                channel = cnx.channel()

                channel.queue_declare(queue='task_finished', exclusive=False)
                res = channel.basic_publish(exchange='',
                                    routing_key='task_finished',
                                    body=value,
                                    properties=pika.BasicProperties(
                                        content_type='application/json',
                                        delivery_mode=1
                                        ))
                cnx.close()
                if res:
                    break
                else:
                    print("send failed try another server")
            except pika.exceptions.AMQPConnectionError as e:
                print(e)
                print("try another server")
            except pika.exceptions.ChannelError as e:
                print(e)
                print("channel error occured...try another server")
        else:
            print("there is something wrong with rabbitmq")



if __name__ == '__main__':
    reducePart = ReducePart()
    reducePart.run()
