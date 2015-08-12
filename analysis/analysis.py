#!/usr/bin/env python
# encoding: utf-8

import pandas as pd
import numpy as np
import sys
sys.path.append('../extract/')
from main import get_logs
import ip_info
import time
import heapq
import json
import store

class Statistics:
    def __init__(self, path, host='localhost', port=6379, db=0):
        self.results = {}
        self.file_name = path
        self.host = host
        self.port = port
        self.db = db

    def analysis(self):
        df = pd.DataFrame()
        print("start time is ", time.clock())
        for log in get_logs(limit=10000):
            print(log)
            if log[7] not in df.columns:
                df[log[7]] = pd.Series()
            vid = log[5].split('.')[0]
            if df[log[7]].get(vid) == None:
                df.loc[vid] = np.nan
                df[log[7]][vid] = 1
            else:
                df[log[7]][vid] += 1
        print("time is ", time.clock())
        for i in df.columns:
            print(i, ":", df[i].sum())
            tmp = df[i].copy()
            tmp.sort(ascending=False)
            for j in range(10):
                print(tmp[j])

    def count(self, l):
        prov = l[5]
        city = l[6]
        if self.results.get(prov) == None:
            self.results[prov] = {}
            self.results[prov]['count'] = {}
            self.results[prov]['total'] = 0
        self.results[prov]['total'] += 1
        prov_count = self.results[prov]['count']
        vid = l[3]
        if prov_count.get(city) == None:
            prov_count[city] = {}
        city_count = prov_count[city]
        if city_count.get(vid) == None:
            city_count[vid] = 0
        city_count[vid] += 1
        l = None


    def get_top(self):
        f = open('output.txt', 'w+')
        res_json = {}
        for prov in self.results:
            if prov == '默认':
                continue
            res_json[prov] = {}
            prov_count = self.results[prov]['count']
            self.results[prov]['top'] = []
            prov_heap = self.results[prov]['top']
            prov_vid_count = {}

            #get top 10 of all city in the prov
            city_top10 = self.get_top_in_city(prov)
            self.results[prov]['city'] = {}
            self.results[prov]['city']['top10'] = city_top10
            res_json[prov]['city'] = {}
            for city in city_top10:
                res_json[prov]['city'][city] = {}
                res_json[prov]['city'][city]['top10'] = []
                for i in city_top10[city]:
                    res_json[prov]['city'][city]['top10'].append({'vid': i[1], 'count': i[0]})

            #count total prov vid
            for city in prov_count:
                for vid in prov_count[city]:
                    if prov_vid_count.get(vid) == None:
                        prov_vid_count[vid] = 0
                    prov_vid_count[vid] += prov_count[city][vid]
            for vid in prov_vid_count:
                if len(prov_heap) < 300:
                    heapq.heappush(prov_heap, (prov_vid_count[vid], vid))
                else:
                    if prov_vid_count[vid] > prov_heap[0][0]:
                        heapq.heapreplace(prov_heap, (prov_vid_count[vid], vid))

            prov_vid_count = None
            prov_count = None

            #get top 10 of all in the prov
            self.results[prov]['top10'] = []
            top300 = heapq.nlargest(300, prov_heap)
            self.results[prov]['top10'] = self.get_valid_top10(top300)
            prov_heap = None

            res_json[prov]['total'] = self.results[prov]['total']
            res_json[prov]['top10'] = []
            print('{', prov, ':')
            for i in self.results[prov]['top10']:
                #res_json[prov]['top10'].append({'name': i[1][0], 'count': i[0], 'type': i[1][1], 'PNG': i[1][2]})
                res_json[prov]['top10'].append({'vid': i[1], 'count': i[0]})
            print(res_json[prov])
            print('\}')
        json.dump(res_json, f, ensure_ascii=False, indent='\t')
        rds = store.RedisStorage(self.host, self.port, self.db)
        rds.store_top(self.file_name, res_json)
        res_json = None
        self.results = None
        f.close()

    def get_top_in_city(self, prov):
        prov_vid_count = self.results[prov]['count']
        city_res = {}
        for city in prov_vid_count:
            if city == '未知' or city == 'NULL':
                continue
            if prov == '北京' and city != '北京市':
                continue
            if prov != '北京' and city == '北京市':
                continue
            city_vid_count = prov_vid_count[city]
            city_top100 = []
            for vid in city_vid_count:
                if len(city_top100) < 100:
                    heapq.heappush(city_top100, (city_vid_count[vid], vid))
                else:
                    if city_vid_count[vid] > city_top100[0][0]:
                        heapq.heapreplace(city_top100, (city_vid_count[vid], vid))
            city_res[city] = self.get_valid_top10(heapq.nlargest(100, city_top100))
        return city_res

    def get_valid_top10(self, top):
        res = []
        top10_count = 0
        for i in top:
            '''
            vid_name = ip_info.get_vedio_info(i[1])
            print(vid_name)
            if self.is_valid(vid_name[0]):
                top10_count += 1
                self.results[prov]['top10'].append((i[0], vid_name))
            '''
            top10_count += 1
            if top10_count >= 10:
                break
            res.append((i[0], i[1]))
        return res

if __name__ == '__main__':
    analysis()

