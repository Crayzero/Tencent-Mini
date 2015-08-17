#!/usr/bin/env python
# encoding: utf-8

import pika
import config
import subprocess
import tarfile
import io
import os
import main
import time

class Worker:
    def __init__(self):
        self.cnx = None
        self.channel = None
        self.connect()

    def connect(self):
        for rabbitmq_server in config.rabbitmq_servers:
            try:
                self.cnx = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_server['host']))
                self.channel = self.cnx.channel()
                self.channel.queue_declare("new_file")
                break
            except pika.exceptions.AMQPConnectionError as err:
                print("cann't connect to the server, try another")
                print(err)
            except pika.exceptions.AMQPError as err:
                print(err)
        else:
            print("error occured when get to rabbitmq!")

    def run(self):
        try:
            print("waiting...")
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(self.callback, queue="new_file")
            self.channel.start_consuming()
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
            print(code)
            if code == 0:
                file_name = file_path.split('/')[-1]
                new_file_path = "../src-logs/" + file_name;
                try:
                    if tarfile.is_tarfile(new_file_path):
                        tar_file = tarfile.open(new_file_path, "r:gz")
                        for tarinfo in tar_file:
                            print(tarinfo.name, " is ", tarinfo.size , " bytes")
                            tar_file.extract(tarinfo, path='../src-logs/')
                            e = main.Extract("../src-logs/" + tarinfo.name)
                            e.extract()
                            print("extract file ", file_path, " finished. ", time.clock())
                            os.remove('../src-logs/' + tarinfo.name)
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    os.remove(new_file_path)
                except tarfile.TarError:
                    print("tarfile error")
                except tarfile.ReadError:
                    print("read tarfile error")
            else:
                #ch.basic_reject(delivery_tag=method.delivery_tag)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
        except (OSError, pika.exceptions.ConnectionClosed) as err:
            #ch.basic_reject(delivery_tag=method.delivery_tag)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(err)
            return ;


if __name__ == '__main__':
    worker = Worker()
    worker.run()
