#!/usr/bin/env python
# encoding: utf-8

log_file = "../src-logs/10.130.28.214_20150720169"

mysql = {
    'user': 'root',
    'password': '111111',
    'host': '127.0.0.1',
    'database': 'tencent',
    'raise_on_warnings': True,
    'charset': 'utf8'
}

prov_to_table = {
        "湖北": "logs_hubei",
        "北京": "logs_beijing",
        "广东": "logs_guangdong",
        "湖南": "logs_hunan",
        "四川": "logs_sichuan",
        "江西": "logs_jiangxi",
        "贵州": "logs_guizhou",
        "陕西": "logs_shanxi",
        "山东": "logs_shandong",
        "甘肃": "logs_gansu",
        "安徽": "logs_anhui",
        "黑龙江": "logs_heilongjiang",
        "上海": "logs_shanghai",
        "浙江": "logs_zhejiang",
        "河南": "logs_henan",
        "山西": "logs_shanxi2",
        "内蒙古": "logs_neimenggu",
        "江苏": "logs_jiangsu",
        "河北": "logs_hebei",
        "新疆": "logs_xinjiang",
        "吉林": "logs_jilin",
        "天津": "logs_tianjin",
        "福建": "logs_fujian",
        "青海": "logs_qinghai",
        "辽宁": "logs_liaonin",
        "重庆": "logs_chongqin",
        "默认": "logs_default",
        "广西": "logs_guangxi",
        "西藏": "logs_xizang",
        "云南": "logs_yunnan",
        "海南": "logs_hainan",
        "宁夏": "logs_ningxia",
        "台湾": "logs_taiwan",
        "香港": "logs_hongkong",
        "澳门": "logs_aomen",
}
