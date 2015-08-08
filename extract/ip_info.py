#!/usr/bin/env python
# encoding: utf-8

import socket
import struct
import requests
from bs4 import BeautifulSoup
import html


def get_ip_info(ip):
    url = "http://ip.chinaz.com/?IP="
    ISPs = ("电信", "联通", "移动")
    if isinstance(ip, str):
        pass
    else:
        sock_addr = struct.pack("=l", ip)
        ip = socket.inet_ntop(socket.AF_INET, sock_addr)
    try:
        res = requests.get(url + ip)
    except requests.exceptions.RequestException as e:
        print(e)
    try:
        print(res.url)
        if res.status_code == 200:
            h = BeautifulSoup(res.text, "html5lib")
            print(h.body.find_all('span', id='status')[0].strong)
            lookup_result = h.body.find_all('span', id='status')[0].strong.string
            localtions = lookup_result.split('==>>')[-1].split(' ')
            if localtions[-1] not in ISPs:
                country = localtions[-2]
                localtion = localtions[-1]
                return (country, localtion)
            else:
                return ("中国", localtions[-2])
    except html.parser.HTMLParseError as e:
        print(e)
    return ()


def get_vedio_info(vid):
    url = "http://openi.video.qq.com/fcgi-bin/vinfo?vid=%s&op_ref=xxx&appkey=xxx"
    if isinstance(vid, str):
        pass
    else:
        vid = '|'.join(vid)
    try:
        rsp = requests.get(url, params={'vid': vid})
        if rsp.status_code == 200:
            res_xml = BeautifulSoup(rsp.text, "lxml")
            videos = res_xml.root.vd.find_all('vi')
            if int(res_xml.root.result.code.string) == 0:
                n = int(res_xml.root.nt.string)
                for i in range(n):
                    video = videos[i]
                    res = (video.n.string, video.t.string, video.png.string)
                    return res
    except requests.exceptions.RequestException as e:
        print(e)
    except Exception as e:
        print(e)
    return ()

class CityService:
    def __init__(self):
        import pickle
        ipd = '../src-logs/ipd.pkl'
        with open(ipd, 'rb') as f:
            self.a = pickle.load(f, encoding='utf-8')
        self.count = 0

    def get_city_by_ip(self, ip):
        net_ip = struct.unpack('!L', socket.inet_pton(socket.AF_INET, ip))[0]
        net_ip = net_ip>>8<<8
        net_ip = str(net_ip)
        city = self.a.get(net_ip)
        if city:
            self.count += 1
            return city
        else:
            return None

    def print_all_ip_city(self):
        for i in self.a:
            print(i, self.a[i])

if __name__ == '__main__':
    cityService = CityService()
    cityService.get_city_by_ip
