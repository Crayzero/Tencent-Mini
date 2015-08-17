#!/usr/bin/env python
# encoding: utf-8

import json
import subprocess
import pika
import config


class Rsync:
    def __init__(self):
        try:
            f = open("processed.txt")
            self.processed = json.load(f)
        except FileNotFoundError :
            self.processed = {}
        self.new_processed = {}

    def rsync_file(self, src, dst):
        cmd = "rsync -n -v " + src + " " + dst + " | grep .tar.gz"
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        res = []
        for line in p.stdout:
            line = line.decode('utf-8')[:-1]
            res.append(line)
        return res

    def get_new_files(self):
        for (src, dst) in zip(config.rsync_src, config.rsync_dst):
            last_slash_index = src.rfind('/')
            src_base_path = src[:last_slash_index+1]
            files = self.rsync_file(src, dst)
            for file in files:
                if self.processed.get(file) == None:
                    self.report_new_file(src_base_path + file)
                self.new_processed[file] = 1
        f = open('processed.txt', 'w')
        json.dump(self.new_processed, f, indent='\t')
        self.processed = None
        self.new_processed = None

    def report_new_file(self, file_path):
        try:
            cnx = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            channel = cnx.channel()

            channel.queue_declare(queue='new_file')
            msg = file_path
            channel.basic_publish(exchange='', routing_key='new_file', body=msg,
                    properties=pika.BasicProperties())
            print('[x] Sent %s'% msg)
            cnx.close()
        except pika.exceptions.AMQPConnectionError as e:
            print("cann't conenct to rabbitmq server")
            raise(e)

if __name__ == '__main__':
    r = Rsync()
    r.get_new_files()
