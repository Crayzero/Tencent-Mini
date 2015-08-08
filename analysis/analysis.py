#!/usr/bin/env python
# encoding: utf-8

import pandas as pd
import numpy as np
import sys
sys.path.append('../extract/')
from main import get_logs
import time

def analysis():
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


if __name__ == '__main__':
    analysis()

