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
                pass
                #self.report_new_result(file_name)
            ch.basic_ack(delivery_tag=method.delivery_tag)

            #ch.basic_cancel('a')
        except Exception as e:
            print(e)
            raise(e)
        finally:
            self.dlm.unlock(self.lock)
            print('unlock the lock')

    def union(self, file_name, part_file_name):
        middle_result = self.redis_client.get(file_name)
        part_result = self.redis_client.get(part_file_name)
        middle_result = json.loads(middle_result.decode('utf-8'))
        part_result = json.loads(part_result.decode('utf-8'))

        start_time = None
        end_time = None

        if middle_result['processed'].get(part_file_name) is not None:
            print(part_file_name, " has been processed")
            return
        middle_result['processed'][part_file_name] = 1

        if middle_result.get('result') == None:
            middle_result['result'] = part_result
        else:
            pass
        if len(middle_result['processed']) == middle_result['length']:
            middle_result = middle_result['result']
            self.report_new_result(file_name, start_time, end_time)
        else:
            self.redis_client._store(file_name, json.dumps(middle_result))
        self.redis_client.delete(part_file_name)


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
            print("there is some thing wrong with rabbitmq")


if __name__ == '__main__':
    reducePart = ReducePart()
    reducePart.run()
