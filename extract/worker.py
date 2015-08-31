#!/usr/bin/env python
# encoding: utf-8

import pika
import config
import subprocess
import tarfile
import os
import main
import time
import zlib
import json
import sys
sys.path.append('../analysis')
import store
from collections import deque

class Worker:
    def __init__(self):
        self.cnx = None
        self.channel = None
        self.connect()
        self.e = main.Extract()
        self.file_deque = deque()

    def connect(self):
        for rabbitmq_server in config.rabbitmq_servers:
            try:
                self.cnx = pika.SelectConnection(pika.ConnectionParameters(host=rabbitmq_server['host']),
                                self.on_connection_open, stop_ioloop_on_close=False)
                break
            except pika.exceptions.AMQPConnectionError as err:
                print("cann't connect to the server, try another")
                print(err)
            except pika.exceptions.AMQPError as err:
                print(err)
        else:
            print("error occured when get to rabbitmq!")

    def on_connection_open(self, unused_connection):
        if config.process_new_file:
            self.cnx.channel(on_open_callback=self.on_channel1_open)
        if config.process_part_file:
            self.cnx.channel(on_open_callback=self.on_channel2_open)
        self.cnx.channel(self.on_subscribe_channel_open)

    def on_channel1_open(self, channel):
        #channel1 process the rsync file
        self.channel1 = channel
        channel.queue_declare(self.on_queue_new_file_open, "new_file")

    def on_channel2_open(self, channel):
        #channel2 process the splited file
        self.channel2 = channel
        channel.queue_declare(self.on_queue_new_part_file_open, 'new_part_file')

    def on_queue_new_file_open(self, method_frame):
        print("waiting for new file ...")
        self.channel1.basic_qos(prefetch_count = 1)
        self.channel1.basic_consume(self.callback, queue="new_file")

    def on_queue_new_part_file_open(self, method_frame):
        print("waiting for new part file ...")
        self.channel2.basic_qos(prefetch_count = 1)
        self.channel2.basic_consume(self.processPartFile, queue='new_part_file')


    def run(self):
        try:
            self.cnx.ioloop.start()
        except pika.exceptions.ConnectionClosed:
            print("connection close, try reconnect")
        except pika.exceptions.AMQPConnectionError:
            print("connection close, try reconnect")

    def callback(self, ch, method, properties, body):
        file_path = body.decode('utf-8')
        rsync_statement = "rsync " + file_path + " ../src-logs/"
        print(rsync_statement)
        try:
            code = subprocess.call(rsync_statement, shell=True)
            if code == 0:
                file_name = file_path.split('/')[-1]
                new_file_path = "../src-logs/" + file_name;
                try:
                    if tarfile.is_tarfile(new_file_path):
                        tar_file = tarfile.open(new_file_path, "r:gz")
                        for tarinfo in tar_file:
                            print(tarinfo.name, " is ", tarinfo.size , " bytes")
                            tar_file.extract(tarinfo, path='../src-logs/')
                            self.split_file("../src-logs/" + tarinfo.name)

                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    os.remove(new_file_path)
                except tarfile.TarError:
                    print("tarfile error")
                    raise(tarfile.TarError('tarfile tar error'))
                except tarfile.ReadError:
                    print("read tarfile error")
                    raise(tarfile.ReadError("read tarfile error"))
            else:
                print("cann't async to file_path")
                ch.basic_reject(delivery_tag=method.delivery_tag)
                #ch.basic_ack(delivery_tag=method.delivery_tag)
                return
        except (OSError, pika.exceptions.ConnectionClosed, zlib.error) as err:
            #ch.basic_reject(delivery_tag=method.delivery_tag)
            os.remove(new_file_path);
            f = open("../src-logs/" + tarinfo.name, 'w')
            f.close()
            print(err)
            self.split_file("../src-logs/" + tarinfo.name)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return ;

    def split_file(self, file_name):
        cmd = "split -l " + str(config.split_lines) + " " + "../src-logs/" + file_name + " " + file_name + "_part_"
        print(cmd)
        try:
            subprocess.call(cmd, shell=True)
        except subprocess.CalledProcessError as e:
            print(e)
            return
        cmd2 = 'ls ' + file_name + '_part_*'
        p = subprocess.Popen(cmd2, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        p.wait()
        if p.returncode == 0:
            files = []
            for line in p.stdout:
                line = line.decode('utf-8')
                file = line[0:-1]
                files.append(file)
            print(files)
            length = len(files)

            rds = store.RedisStorage()
            rds.store_part_length(file_name.split('/')[-1], length)

            #if file is split remove the old file
            os.remove(file_name)
            for file_path in files:
                #if received finished processed Message, just to delete the split file
                self.file_deque.append(file_path.split('/')[-1])
                #self.subscribe_finish()
                self.reportNewPartFile(file_path, length)
        else:
            #if file cann't be split, reserve the old file
            #once received finish processed Message, delete the tmp zero size file
            self.file_deque.append(file_name.split('/')[-1])
            #self.subscribe_finish()

            rds = store.RedisStorage()
            rds.store_part_length(file_name.split('/')[-1],  0)

            self.reportNewPartFile(file_name, 0)


    def reportNewPartFile(self, file_path, length):
        for server in config.rabbitmq_servers:
            try:
                cnx = pika.BlockingConnection(pika.ConnectionParameters(host=server['host']))
                channel = cnx.channel()
                msg = {}
                msg['length'] = length
                msg['file_path'] = file_path

                channel.queue_declare(queue='new_part_file', exclusive=False)
                res = channel.basic_publish(exchange='',
                                routing_key='new_part_file',
                                body=json.dumps(msg),
                                properties=pika.BasicProperties(
                                    content_type='application/json',
                                    delivery_mode=1
                                    ))
                cnx.close()
                if res:
                    break
                else:
                    print("Message was returned, try another server.")
            except pika.exceptions.AMQPConnectionError as err:
                print(err)
            except pika.exceptions.ChannelError as err:
                print(err)
        else:
            print("cann't connect to the rabbitmq server")

    def reportPartFileFinished(self, file_path):
        for server in config.rabbitmq_servers:
            try:
                cnx = pika.BlockingConnection(pika.ConnectionParameters(host=server['host']))
                channel = cnx.channel()
                msg = file_path

                channel.exchange_declare(exchange='part_file_finished_to_remove', type='direct')
                res = channel.basic_publish(exchange='part_file_finished_to_remove',
                                routing_key=file_path.split('/')[-1],
                                body=msg,
                                properties=pika.BasicProperties(
                                    content_type='application/json',
                                    delivery_mode=1
                                    ))
                cnx.close()
                if res:
                    break
                else:
                    print("Message was returned, try another server.")
            except pika.exceptions.AMQPConnectionError as err:
                print(err)
            except pika.exceptions.ChannelError as err:
                print(err)
        else:
            print("cann't connect to the rabbitmq server")


    def processPartFile(self, ch, method, properties, body):
        msg = body.decode('utf-8')
        print(msg)
        msg_json = json.loads(msg)
        file = msg_json['file_path']
        length = msg_json['length']

        file_path = os.path.abspath(file)
        cmd = 'rsync ' + file_path + " " + "../src-logs"
        print(cmd)

        try:
            subprocess.check_call(cmd, shell=True)
            file_name = file.split('/')[-1]
            self.e.extract("../src-logs/" + file_name, length)
            print("extract file ", file_name, " finished. ", time.clock())
            os.remove("../src-logs/" + file.split('/')[-1])

            self.reportPartFileFinished(file_name)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except subprocess.CalledProcessError as e:
            print(e)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            #ch.basic_reject(delivery_tag=method.delivery_tag)

    def subscribe_finish(self):
        print("subscibing...")
        self.subscribe_channel.exchange_declare(self.on_subscribe_exchange_declareok, "part_file_finished_to_remove", "direct")

    def on_subscribe_channel_open(self, channel):
        self.subscribe_channel = channel

    def on_subscribe_exchange_declareok(self, unused_frame):
        print("exchange declare ok")
        if len(self.file_deque):
            file_name = self.file_deque[0]
        else:
            return
        self.subscribe_channel.queue_declare(self.on_subscribe_queue_declareok, file_name)

    def on_subscribe_queue_declareok(self, method_frame):
        print("queue name is ", method_frame.method.queue)
        if len(self.file_deque):
            file_name = self.file_deque[0]
        else:
            return
        self.subscribe_channel.queue_bind(self.on_subscribe_queue_bindok, method_frame.method.queue, "part_file_finished_to_remove", file_name)

    def on_subscribe_queue_bindok(self, unused_frame):
        if len(self.file_deque):
            queue_name = self.file_deque.popleft()
        else:
            return

        print("waiting on queue:", queue_name)
        self.subscribe_channel.basic_consume(self.on_subcscribe_message, queue_name)

    def on_subcscribe_message(self, channel, basic_deliver,properties, body):
        body = body.decode('utf-8')
        if os.path.exists('../src-logs/' + body):
            os.remove('../src-logs/+' + body)
        channel.basic_ack(basic_deliver.delivery_tag)


import unittest

class TestMethods(unittest.TestCase):
    def setUp(self):
        self.worker = Worker()

    def test_split(self):
        self.worker.split_file(config.log_file)

    @unittest.skip("test for None split file")
    def test_run(self):
        self.worker.run()


if __name__ == '__main__':
    #for unittest
    #unittest.main()

    worker = Worker()
    worker.run()
