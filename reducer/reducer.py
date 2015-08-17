#!/usr/bin/env python
# encoding: utf-8

import pika
import redlock
import sys
sys.path.append('../extract')
import config
import ip_info
import time
import random
import json
sys.path.append("../analysis")
import store
import heapq
import re


class Reduce:
    def __init__(self):
        self.lock_time = 5000
        self.dlm = redlock.Redlock(config.redis_servers)
        self.resource = "reduce"
        self.lock = None
        self.queue = 'task_finished'
        self.redis_client = store.RedisStorage()
        self.redis_final_result_key = "result"
        self.total = 12
        pass

    def callback(self, ch, method, properties, body):
        try:
            msg = body.decode('utf-8')
            try:
                msg = json.loads(msg)
            except Exception as e:
                print(e)
                ch.basic_reject(delivery_tag=method.delivery_tag)
            key = msg['key']
            print(key)
            logfile_ip = key.split('_')[0]
            log_num = key.split('_')[1]

            #get lock of log_num key in redis
            while True:
                self.lock = self.dlm.lock("lock_" + str(log_num), self.lock_time)
                if not self.lock:
                    time.sleep(random.uniform(1,3))
                else:
                    print("acquired lock")
                    break

            map_result = self.redis_client.get_map_result(key)
            if map_result !=  None:
                self.union(map_result, logfile_ip, log_num)
            self.redis_client.delete(key)
            map_result = None
            ch.basic_ack(delivery_tag = method.delivery_tag)
        except Exception as e:
            ch.basic_reject(delivery_tag=method.delivery_tag)
            raise(e)
        finally:
            self.dlm.unlock(self.lock)
            print("unlock the lock")
            print("finished union\n")

    def run(self):
        cnx = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = cnx.channel()

        channel.queue_declare(queue=self.queue)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(self.callback, queue=self.queue)

        print("waiting for result")
        channel.start_consuming()

    def union(self, map_result, logfile_ip, log_num):
        print("union to ", log_num)
        res = self.redis_client.get_map_result(log_num)
        if res is None:
            res = map_result
            res["processed"] = []
            res["processed"].append(logfile_ip)
            self.redis_client.store(log_num, json.dumps(res))
        else:
            print(res["processed"])
            if logfile_ip in res["processed"]:
                print("log_file " + logfile_ip + '_' + log_num + " has been processed")
            else:
                for prov in map_result:
                    if prov == 'processed':
                        continue
                    map_result_prov = map_result[prov]
                    if res.get(prov) == None:
                        res[prov] = {}
                        res[prov]['total'] = 0
                        res[prov]['count'] = {}
                        res[prov]['city'] = {}
                    res[prov]['total'] += map_result_prov['total']
                    for vid in map_result_prov['count']:
                        if res[prov]['count'].get(vid) == None:
                            res[prov]['count'][vid] = 0
                        prov_one_vid_sum = res[prov]['count'][vid] + map_result_prov['count'][vid]
                        if  prov_one_vid_sum > 1:
                            res[prov]['count'][vid] = prov_one_vid_sum
                    for city in map_result_prov['city']:
                        if res[prov]['city'].get(city) == None:
                            res[prov]['city'][city] = {}
                        if res[prov]['city'][city].get('count') == None:
                            res[prov]['city'][city]['count'] = 0
                        prov_city_sum = res[prov]['city'][city]['count'] + map_result_prov['city'][city]['count']
                        if prov_city_sum > 1:
                            res[prov]['city'][city]['count'] = prov_city_sum
                res["processed"].append(logfile_ip)
        if self.reduced_all(res):
            self.report_new_result(res, log_num)
        else:
            self.redis_client.store(log_num, json.dumps(res))

    def reduced_all(self, res):
        if len(res['processed']) == self.total:
            if len(res) == 1:
                return True
            for prov in res:
                if prov == 'processed':
                    continue
                prov_top300 = self.get_top300(res[prov]['count'])
                print('{ prov: ', prov)
                prov_top10 = self.get_valid_top10(prov_top300)
                #print(prov_top10)
                print('}\n')
                res[prov]['count'] = None
                res[prov]['top10'] = prov_top10
            return True
        return False

    def report_new_result(self, res, log_num):
        processed = {}
        processed['processed'] = res['processed']
        res['processed'] = None
        self.redis_client.store(self.redis_final_result_key, json.dumps(res))
        res = None
        self.redis_client.store(log_num, json.dumps(processed), 300)

        for rabbitmq_server in config.rabbitmq_servers:
            try:
                cnx = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_server['host']))
                channel = cnx.channel()
                channel.exchange_declare(exchange='new_result', type="fanout")
                channel.basic_publish(exchange='new_result', routing_key='',body="new result")
                cnx.close()
            except pika.exceptions.AMQPConnectionError:
                pass

    def get_top300(self, prov_vid_counts):
        vid_counts_heap = []
        for vid in prov_vid_counts:
            if len(vid_counts_heap) < 300:
                heapq.heappush(vid_counts_heap, (prov_vid_counts[vid], vid))
            else:
                if prov_vid_counts[vid] > vid_counts_heap[0][0]:
                    heapq.heapreplace(vid_counts_heap, (prov_vid_counts[vid], vid))
        top300 = heapq.nlargest(300, vid_counts_heap)
        vid_counts_heap = None
        return top300

    def get_valid_top10(self, tops):
        res = []
        top10_count = 0
        for i in tops:
            vid_name = ip_info.get_vedio_info(i[1])
            if not vid_name:
                continue
            if self.is_valid(vid_name[0]):
                top10_count += 1
                res.append((i[0], vid_name))
            ''' don't get video name
            top10_count += 1
            res.append((i[0], i[1]))
            '''
            if top10_count >= 10:
                break
        return res

    def is_valid(self, name):
        if re.match('\d+\w+', name):
            return False
        if re.match('[a-z]+\d+', name):
            return False
        if re.match('\w+\.flv\Z', name):
            return False
        if re.match('预告片.*', name):
            return False
        if re.match(".*\.mp4", name):
            return False
        return True


def test():
    reduce = Reduce()
    reduce.run()

if __name__ == '__main__':
    test()
