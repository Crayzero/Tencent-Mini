#!/usr/bin/env python
# encoding: utf-8

import pika
import redlock
import sys
sys.path.append('../extract')
import config
import time
import random


class Reduce:
    def __init__(self):
        self.lock_time = 5000
        self.dlm = redlock.Redlock(config.redis_servers)
        self.resource = "reduce"
        self.lock = None
        self.queue = 'task_finished'
        pass

    def callback(self, ch, method, properties, body):
        try:
            while True:
                self.lock = self.dlm.lock(self.resource, self.lock_time)
                if not self.lock:
                    time.sleep(random.uniform(1,3))
                else:
                    print("acquired lock")
                    break
            print(body)
            ch.basic_ack(delivery_tag = method.delivery_tag)
        except Exception as e:
            print(e)
            raise(e)
        finally:
            self.dlm.unlock(self.lock)

    def run(self):
        cnx = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = cnx.channel()

        channel.queue_declare(queue=self.queue)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(self.callback, queue=self.queue)

        channel.start_consuming()

def test():
    reduce = Reduce()
    reduce.run()

if __name__ == '__main__':
    test()
