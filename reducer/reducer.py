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
import datetime


class Reduce:
    def __init__(self):
        self.lock_time = 5000
        self.dlm = redlock.Redlock(config.redis_servers)
        self.resource = "reduce"
        self.lock = None
        self.queue = 'task_finished'
        self.redis_client = store.RedisStorage()
        self.redis_final_result_key = "result"
        self.total = 31
        pass

    def callback(self, ch, method, properties, body):
        try:
            msg = body.decode('utf-8')
            try:
                msg = json.loads(msg)
            except Exception as e:
                print(e)
                ch.basic_reject(delivery_tag=method.delivery_tag)
            print(msg)
            key = msg['key']
            start_time = msg['start_time']
            end_time = msg['end_time']
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
            if map_result.get('result') is not None:
                map_result = map_result['result']

            self.union(map_result, logfile_ip, log_num, start_time, end_time)
            self.redis_client.delete(key)
            map_result = None
            ch.basic_ack(delivery_tag = method.delivery_tag)
        except Exception as e:
            #ch.basic_ack(delivery_tag = method.delivery_tag)
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

    def union(self, map_result, logfile_ip, log_num, start_time, end_time):
        print("union to ", log_num)
        processed = False
        if start_time is not None:
            start_time_date = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        if end_time is not None:
            end_time_date = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        if start_time is not None:
            print((end_time_date - start_time_date).seconds)

        res = self.redis_client.get_map_result(log_num)
        if res is None or len(res) == 0:
            res = map_result
            if res == None:
                res = {}
            res["processed"] = []
            res["processed"].append(logfile_ip)
            res['time'] = {}
            if start_time is not None and (end_time_date - start_time_date).seconds == 299:
                res['time']['start_time'] = start_time
                res['time']['end_time'] = end_time
            self.redis_client.store(log_num, json.dumps(res))
        else:
            print(res["processed"])

            if logfile_ip in res["processed"]:
                print("log_file " + logfile_ip + '_' + log_num + " has been processed")
                processed = True
            elif map_result == None or len(map_result) == 0:
                res["processed"].append(logfile_ip)
            else:
                if res.get('time') is None:
                    res['time'] = {}
                if len(res['time']) == 0 and start_time is not None and (end_time_date - start_time_date).seconds == 299:
                    res['time']['start_time'] = start_time
                    res['time']['end_time'] = end_time

                if res.get('count_per_five_second') == None:
                    res['count_per_five_second'] = [0] * 60
                print(len(res['count_per_five_second']))
                for i in range(min(len(map_result['count_per_five_second']), 60)):
                    res['count_per_five_second'][i] += map_result['count_per_five_second'][i]

                for prov in map_result:
                    if prov == 'processed':
                        continue
                    if prov == 'count_per_five_second':
                        continue
                    if prov == 'source':
                        continue
                    map_result_prov = map_result[prov]
                    if res.get(prov) == None:
                        res[prov] = {}
                        res[prov]['total'] = 0
                        res[prov]['count'] = {}
                        res[prov]['city'] = {}
                    res[prov]['total'] += map_result_prov['total']
                    #merge the vid and count of vid
                    for vid in map_result_prov['count']:
                        if res[prov]['count'].get(vid) == None:
                            res[prov]['count'][vid] = 0
                        prov_one_vid_sum = res[prov]['count'][vid] + map_result_prov['count'][vid]
                        if  prov_one_vid_sum > 1:
                            res[prov]['count'][vid] = prov_one_vid_sum
                        else:
                            res[prov]['count'].pop(vid)

                    #for each city in prov, merge the total count in city
                    for city in map_result_prov['city']:
                        if city == None:
                            continue
                        if res[prov]['city'].get(city) == None:
                            res[prov]['city'][city] = {}
                        if res[prov]['city'][city].get('count') == None:
                            res[prov]['city'][city]['count'] = 0
                        prov_city_sum = res[prov]['city'][city]['count'] + map_result_prov['city'][city]['count']
                        if prov_city_sum > 1:
                            res[prov]['city'][city]['count'] = prov_city_sum

                for source_city in map_result['source']:
                    if res.get('source') == None:
                        res['source'] = {}
                    if res['source'].get(source_city) == None:
                        res['source'][source_city] = map_result['source'][source_city]
                    else:
                        if res['source'][source_city].get('total') == None:
                            res['source'][source_city]['total'] = 0
                        res['source'][source_city]['total'] += map_result['source'][source_city]['total']
                        for to_city in map_result['source'][source_city]:
                            if to_city == 'total':
                                continue
                            if res['source'][source_city].get(to_city) == None:
                                res['source'][source_city][to_city] = map_result['source'][source_city][to_city]
                            else:
                                res['source'][source_city][to_city] += map_result['source'][source_city][to_city]
                            if res['source'][source_city][to_city] < 10:
                                res['source'][source_city].pop(to_city)

                res["processed"].append(logfile_ip)

        if not processed:
            if self.reduced_all(res):
                print('union all')
                self.report_new_result(res, log_num)
            else:
                self.redis_client.store(log_num, json.dumps(res))

    def reduced_all(self, res):
        if len(res['processed']) == self.total:
            if len(res) == 1:
                return True
            country_top = {}
            for prov in res:
                if prov == 'processed':
                    continue
                if prov == '未知':
                    continue
                if prov == 'count_per_five_second':
                    continue
                if prov == 'source':
                    continue
                if prov == 'time':
                    continue
                prov_top300 = self.get_top300(res[prov]['count'])

                self.get_country_top(country_top, prov_top300)

                print('{ prov: ', prov)
                prov_top10 = self.get_valid_top10(prov_top300)
                print('}\n')
                #res[prov]['count'] = None
                #res[prov].pop('count')
                res[prov]['top10'] = prov_top10
            country_top300 = self.get_top300(country_top)
            country_top10_vid = []
            country_top10 = self.get_valid_top10(country_top300, country_top10_vid)
            print(country_top10_vid)
            prov_vid_counts = {}
            for vid in country_top10_vid:
                prov_vid_counts[vid] = {}
                for prov in res:
                    if isinstance(res[prov], dict) and res[prov].get('count') is not None:
                        if res[prov]['count'].get(vid) is not None:
                            prov_vid_counts[vid][prov] = res[prov]['count'][vid]

            for prov in res:
                if isinstance(res[prov], dict) and res[prov].get('count') is not None:
                    res[prov].pop('count')

            res['top10'] = {}
            res['top10']['name'] = country_top10
            res['top10']['distribute'] = prov_vid_counts

            if res.get('source') is None:
                res['source'] = {}
            for city in res['source']:
                source_city_top_10 = self.get_source_top10(res['source'][city])
                total = res['source'][city]['total']
                res['source'][city] = {}
                res['source'][city]['top10'] = source_city_top_10
                res['source'][city]['total'] = total
            return True
        return False

    def report_new_result(self, res, log_num):
        processed = {}
        processed['processed'] = res['processed']
        res['processed'] = None
        res['extra'] = {}
        res['extra']['log_num'] = log_num
        if res.get('未知') != None:
            res.pop('未知')
        res.pop('processed')
        self.redis_client.store(self.redis_final_result_key, json.dumps(res))

        ''' write result to a file
        f = open('output.txt', 'w')
        json.dump(res, f, ensure_ascii=False, indent='\t')
        f.close()
        '''

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

    def get_country_top(self, country_top, prov_top300):
        for i in prov_top300:
            if country_top.get(i[1]) == None:
                country_top[i[1]] = i[0]
            else:
                country_top[i[1]] += i[0]


    def get_source_top10(self, sources):
        source_count_heap = []
        for city in sources:
            if city == 'total':
                continue
            if city == '未知':
                continue
            if city == None or city == 'NULL':
                continue
            if len(source_count_heap) < 30:
                heapq.heappush(source_count_heap, (sources[city], city))
            else:
                if sources[city] > source_count_heap[0][0]:
                    heapq.heapreplace(source_count_heap, (sources[city], city))
        return heapq.nlargest(30, source_count_heap)

    def get_valid_top10(self, tops, top10_vid=None):
        if top10_vid is None:
            top10_vid = []

        res = []
        top10_count = 0
        f = open("vid.txt", 'w+')
        for i in tops:
            f.write(i[1] + '\t' + str(i[0]))
            f.write('\n')
            vid_name = ip_info.get_vedio_info(i[1])
            if not vid_name:
                continue
            if self.is_valid(vid_name[0]):
                top10_count += 1
                top10_vid.append(i[1])
                res.append((i, vid_name))
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
        if re.match('(\w+/?)+\.flv', name):
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
