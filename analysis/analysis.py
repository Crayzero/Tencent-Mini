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

class Statistics:
    def __init__(self):
        self.results = {}

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
        if self.results.get(prov) == None:
            self.results[prov] = {}
            self.results[prov]['count'] = {}
            self.results[prov]['total'] = 0
        self.results[prov]['total'] += 1
        prov_count = self.results[prov]['count']
        vid = l[3]
        if prov_count.get(vid) == None:
            prov_count[vid] = 0
        prov_count[vid] += 1
        l = None

    def get_top(self):
        for prov in self.results:
            prov_count = self.results[prov]['count']
            self.results[prov]['top'] = []
            prov_heap = self.results[prov]['top']
            for vid in prov_count:
                if len(prov_heap) < 3000:
                    heapq.heappush(prov_heap, (prov_count[vid], vid))
                else:
                    if prov_count[vid] > prov_heap[0][0]:
                        heapq.heapreplace(prov_heap, (prov_count[vid], vid))
            prov_count = None
            self.results[prov]['top10'] = []
            top10 = heapq.nlargest(10, prov_heap)
            for i in top10:
                #vid_name = ip_info.get_vedio_info(i[1])
                #print(vid_name)
                self.results[prov]['top10'].append((i[0], vid))
            prov_heap = None
            print('{', prov, ':')
            print('total:', self.results[prov]['total'])
            for i in self.results[prov]['top10']:
                print(i)
            print('}')

if __name__ == '__main__':
    analysis()

