# coding=utf-8
# 首日启动,什么都不做,轮空
# 次日启动,每天抓取昨天的相应数据(如6月1日抓取5月31日的)

import requests
import json
import datetime
import time
import os,sys
import logging
#from DB import *
from multiprocessing import Pool

def get_current_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def calc_next_time(livestraming_time, duration):
    return (datetime.datetime.strptime(livestraming_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(minutes=float(duration))).strftime("%Y-%m-%d %H:%M:%S")

def time_comparison(left_time, right_time):
    return datetime.datetime.strptime(left_time, "%Y-%m-%d %H:%M:%S") <= datetime.datetime.strptime(right_time, "%Y-%m-%d %H:%M:%S")

today_date = datetime.datetime.now().strftime("%Y-%m-%d")
lastday_date = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
first_crawl_date = (datetime.datetime.now()+datetime.timedelta(days=-121)).strftime("%Y-%m-%d")

with open('/root/xd_crawler/cookie','r') as f:
    cookie = f.read().strip()
with open('/root/xd_crawler/token','r') as f:
    token = f.read().strip()
with open('/root/xd_crawler/type','r') as f:
    input_type = f.read().strip().split()

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'zh-CN,zh;q=0.9',
    'content-type': 'application/json;charset=UTF-8',
    'cookie': cookie,
    'n-token': token,
    'origin': 'https://xd.newrank.cn',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36',
}

# def calc_userAvgDuration(start_time,end_time,point_list, type,num_zb,url_zbjl):
#     try:
#         start_timestamp = time.mktime(time.strptime(start_time, "%Y-%m-%d %H:%M:%S"))
#         end_timestamp = time.mktime(time.strptime(end_time, "%Y-%m-%d %H:%M:%S"))
#         total_time = end_timestamp - start_timestamp
#
#         user_count = []
#         start_cumulative_number_online = 0
#         end_cumulative_number_online = 0
#         for point in point_list:
#             if point.get('crawl_date') == start_time and len(user_count) == 0 :
#                 user_count.append(point.get('user_count'))
#                 start_cumulative_number_online = point.get('stats_total_user')
#             if point.get('crawl_date') != start_time and len(user_count) != 0:
#                 if point.get('user_count')==None or point.get('stats_total_user')==None:
#                     user_count.append(0)
#                 else:
#                     user_count.append(point.get('user_count'))
#                 if point.get('crawl_date') == end_time:
#                     end_cumulative_number_online = point.get('stats_total_user')
#                     break
#
#         tlsc = total_time * (sum(user_count)/len(user_count)/(end_cumulative_number_online - start_cumulative_number_online))
#
#
#         pos = str(tlsc).index('.')
#         tlsc = float(str(tlsc)[:pos+3])
#
#         return '{:.2f}'.format(tlsc)
#     except:
#         logging.info('[*] Get zbjl_tlsc... calc_userAvgDuration failed. type:%s, num_zb:%s, url_zbjl:%s, start_time:%s | end_time:%s at %s' % (type, num_zb, url_zbjl, start_time, end_time, get_current_time()))
#         return '--'


def run_crawler_task(type, current_taks):

    #for type in input_type:

    from peewee import MySQLDatabase
    from peewee import Model, CharField
    import pymysql
    from warnings import filterwarnings
    filterwarnings('ignore', category=pymysql.Warning)

    db1 = MySQLDatabase('xd', user='root', password='Wanghongpeng1', host='127.0.0.1', port=3306)  # ,charset='utf8mb4')
    db1.connection()

    class BaseModel(Model):
        class Meta:
            database = db1

    class list_ms_zbjl(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)
        theme = CharField(max_length=256, null=False)

        duration = CharField(max_length=256, null=False)
        yinlangshouru = CharField(max_length=256, null=False)
        benchangyinlang = CharField(max_length=256, null=False)
        songli = CharField(max_length=256, null=False)
        zaixianfengzhi = CharField(max_length=256, null=False)
        leijiguankan = CharField(max_length=256, null=False)
        zongdianzan = CharField(max_length=256, null=False)
        danchangzhangfen = CharField(max_length=256, null=False)
        zhuanfenlv = CharField(max_length=256, null=False)
        pingjunzaixian = CharField(max_length=256, null=False)
        pingjunzhiliu = CharField(max_length=256, null=False)
        yuguxiaoshoue = CharField(max_length=256, null=False)
        yuguxiaoshouliang = CharField(max_length=256, null=False)
        shangjiashangpin = CharField(max_length=256, null=False)
        zuigaodanjia = CharField(max_length=256, null=False)
        zuigaoxiaoliang = CharField(max_length=256, null=False)
        zuigaoxiaoshoue = CharField(max_length=256, null=False)
        kedanjia = CharField(max_length=256, null=False)
        renjungoumaijiazhi = CharField(max_length=256, null=False)
        xiaoshouzhuanhualv = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_ss_zbjl(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)
        theme = CharField(max_length=256, null=False)

        duration = CharField(max_length=256, null=False)
        yinlangshouru = CharField(max_length=256, null=False)
        benchangyinlang = CharField(max_length=256, null=False)
        songli = CharField(max_length=256, null=False)
        zaixianfengzhi = CharField(max_length=256, null=False)
        leijiguankan = CharField(max_length=256, null=False)
        zongdianzan = CharField(max_length=256, null=False)
        danchangzhangfen = CharField(max_length=256, null=False)
        zhuanfenlv = CharField(max_length=256, null=False)
        pingjunzaixian = CharField(max_length=256, null=False)
        pingjunzhiliu = CharField(max_length=256, null=False)
        yuguxiaoshoue = CharField(max_length=256, null=False)
        yuguxiaoshouliang = CharField(max_length=256, null=False)
        shangjiashangpin = CharField(max_length=256, null=False)
        zuigaodanjia = CharField(max_length=256, null=False)
        zuigaoxiaoliang = CharField(max_length=256, null=False)
        zuigaoxiaoshoue = CharField(max_length=256, null=False)
        kedanjia = CharField(max_length=256, null=False)
        renjungoumaijiazhi = CharField(max_length=256, null=False)
        xiaoshouzhuanhualv = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_kj_zbjl(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)
        theme = CharField(max_length=256, null=False)

        duration = CharField(max_length=256, null=False)
        yinlangshouru = CharField(max_length=256, null=False)
        benchangyinlang = CharField(max_length=256, null=False)
        songli = CharField(max_length=256, null=False)
        zaixianfengzhi = CharField(max_length=256, null=False)
        leijiguankan = CharField(max_length=256, null=False)
        zongdianzan = CharField(max_length=256, null=False)
        danchangzhangfen = CharField(max_length=256, null=False)
        zhuanfenlv = CharField(max_length=256, null=False)
        pingjunzaixian = CharField(max_length=256, null=False)
        pingjunzhiliu = CharField(max_length=256, null=False)
        yuguxiaoshoue = CharField(max_length=256, null=False)
        yuguxiaoshouliang = CharField(max_length=256, null=False)
        shangjiashangpin = CharField(max_length=256, null=False)
        zuigaodanjia = CharField(max_length=256, null=False)
        zuigaoxiaoliang = CharField(max_length=256, null=False)
        zuigaoxiaoshoue = CharField(max_length=256, null=False)
        kedanjia = CharField(max_length=256, null=False)
        renjungoumaijiazhi = CharField(max_length=256, null=False)
        xiaoshouzhuanhualv = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_yl_zbjl(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)
        theme = CharField(max_length=256, null=False)

        duration = CharField(max_length=256, null=False)
        yinlangshouru = CharField(max_length=256, null=False)
        benchangyinlang = CharField(max_length=256, null=False)
        songli = CharField(max_length=256, null=False)
        zaixianfengzhi = CharField(max_length=256, null=False)
        leijiguankan = CharField(max_length=256, null=False)
        zongdianzan = CharField(max_length=256, null=False)
        danchangzhangfen = CharField(max_length=256, null=False)
        zhuanfenlv = CharField(max_length=256, null=False)
        pingjunzaixian = CharField(max_length=256, null=False)
        pingjunzhiliu = CharField(max_length=256, null=False)
        yuguxiaoshoue = CharField(max_length=256, null=False)
        yuguxiaoshouliang = CharField(max_length=256, null=False)
        shangjiashangpin = CharField(max_length=256, null=False)
        zuigaodanjia = CharField(max_length=256, null=False)
        zuigaoxiaoliang = CharField(max_length=256, null=False)
        zuigaoxiaoshoue = CharField(max_length=256, null=False)
        kedanjia = CharField(max_length=256, null=False)
        renjungoumaijiazhi = CharField(max_length=256, null=False)
        xiaoshouzhuanhualv = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_cy_zbjl(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)
        theme = CharField(max_length=256, null=False)

        duration = CharField(max_length=256, null=False)
        yinlangshouru = CharField(max_length=256, null=False)
        benchangyinlang = CharField(max_length=256, null=False)
        songli = CharField(max_length=256, null=False)
        zaixianfengzhi = CharField(max_length=256, null=False)
        leijiguankan = CharField(max_length=256, null=False)
        zongdianzan = CharField(max_length=256, null=False)
        danchangzhangfen = CharField(max_length=256, null=False)
        zhuanfenlv = CharField(max_length=256, null=False)
        pingjunzaixian = CharField(max_length=256, null=False)
        pingjunzhiliu = CharField(max_length=256, null=False)
        yuguxiaoshoue = CharField(max_length=256, null=False)
        yuguxiaoshouliang = CharField(max_length=256, null=False)
        shangjiashangpin = CharField(max_length=256, null=False)
        zuigaodanjia = CharField(max_length=256, null=False)
        zuigaoxiaoliang = CharField(max_length=256, null=False)
        zuigaoxiaoshoue = CharField(max_length=256, null=False)
        kedanjia = CharField(max_length=256, null=False)
        renjungoumaijiazhi = CharField(max_length=256, null=False)
        xiaoshouzhuanhualv = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_ty_zbjl(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)
        theme = CharField(max_length=256, null=False)

        duration = CharField(max_length=256, null=False)
        yinlangshouru = CharField(max_length=256, null=False)
        benchangyinlang = CharField(max_length=256, null=False)
        songli = CharField(max_length=256, null=False)
        zaixianfengzhi = CharField(max_length=256, null=False)
        leijiguankan = CharField(max_length=256, null=False)
        zongdianzan = CharField(max_length=256, null=False)
        danchangzhangfen = CharField(max_length=256, null=False)
        zhuanfenlv = CharField(max_length=256, null=False)
        pingjunzaixian = CharField(max_length=256, null=False)
        pingjunzhiliu = CharField(max_length=256, null=False)
        yuguxiaoshoue = CharField(max_length=256, null=False)
        yuguxiaoshouliang = CharField(max_length=256, null=False)
        shangjiashangpin = CharField(max_length=256, null=False)
        zuigaodanjia = CharField(max_length=256, null=False)
        zuigaoxiaoliang = CharField(max_length=256, null=False)
        zuigaoxiaoshoue = CharField(max_length=256, null=False)
        kedanjia = CharField(max_length=256, null=False)
        renjungoumaijiazhi = CharField(max_length=256, null=False)
        xiaoshouzhuanhualv = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_gx_zbjl(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)
        theme = CharField(max_length=256, null=False)

        duration = CharField(max_length=256, null=False)
        yinlangshouru = CharField(max_length=256, null=False)
        benchangyinlang = CharField(max_length=256, null=False)
        songli = CharField(max_length=256, null=False)
        zaixianfengzhi = CharField(max_length=256, null=False)
        leijiguankan = CharField(max_length=256, null=False)
        zongdianzan = CharField(max_length=256, null=False)
        danchangzhangfen = CharField(max_length=256, null=False)
        zhuanfenlv = CharField(max_length=256, null=False)
        pingjunzaixian = CharField(max_length=256, null=False)
        pingjunzhiliu = CharField(max_length=256, null=False)
        yuguxiaoshoue = CharField(max_length=256, null=False)
        yuguxiaoshouliang = CharField(max_length=256, null=False)
        shangjiashangpin = CharField(max_length=256, null=False)
        zuigaodanjia = CharField(max_length=256, null=False)
        zuigaoxiaoliang = CharField(max_length=256, null=False)
        zuigaoxiaoshoue = CharField(max_length=256, null=False)
        kedanjia = CharField(max_length=256, null=False)
        renjungoumaijiazhi = CharField(max_length=256, null=False)
        xiaoshouzhuanhualv = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_ly_zbjl(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)
        theme = CharField(max_length=256, null=False)

        duration = CharField(max_length=256, null=False)
        yinlangshouru = CharField(max_length=256, null=False)
        benchangyinlang = CharField(max_length=256, null=False)
        songli = CharField(max_length=256, null=False)
        zaixianfengzhi = CharField(max_length=256, null=False)
        leijiguankan = CharField(max_length=256, null=False)
        zongdianzan = CharField(max_length=256, null=False)
        danchangzhangfen = CharField(max_length=256, null=False)
        zhuanfenlv = CharField(max_length=256, null=False)
        pingjunzaixian = CharField(max_length=256, null=False)
        pingjunzhiliu = CharField(max_length=256, null=False)
        yuguxiaoshoue = CharField(max_length=256, null=False)
        yuguxiaoshouliang = CharField(max_length=256, null=False)
        shangjiashangpin = CharField(max_length=256, null=False)
        zuigaodanjia = CharField(max_length=256, null=False)
        zuigaoxiaoliang = CharField(max_length=256, null=False)
        zuigaoxiaoshoue = CharField(max_length=256, null=False)
        kedanjia = CharField(max_length=256, null=False)
        renjungoumaijiazhi = CharField(max_length=256, null=False)
        xiaoshouzhuanhualv = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_jk_zbjl(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)
        theme = CharField(max_length=256, null=False)

        duration = CharField(max_length=256, null=False)
        yinlangshouru = CharField(max_length=256, null=False)
        benchangyinlang = CharField(max_length=256, null=False)
        songli = CharField(max_length=256, null=False)
        zaixianfengzhi = CharField(max_length=256, null=False)
        leijiguankan = CharField(max_length=256, null=False)
        zongdianzan = CharField(max_length=256, null=False)
        danchangzhangfen = CharField(max_length=256, null=False)
        zhuanfenlv = CharField(max_length=256, null=False)
        pingjunzaixian = CharField(max_length=256, null=False)
        pingjunzhiliu = CharField(max_length=256, null=False)
        yuguxiaoshoue = CharField(max_length=256, null=False)
        yuguxiaoshouliang = CharField(max_length=256, null=False)
        shangjiashangpin = CharField(max_length=256, null=False)
        zuigaodanjia = CharField(max_length=256, null=False)
        zuigaoxiaoliang = CharField(max_length=256, null=False)
        zuigaoxiaoshoue = CharField(max_length=256, null=False)
        kedanjia = CharField(max_length=256, null=False)
        renjungoumaijiazhi = CharField(max_length=256, null=False)
        xiaoshouzhuanhualv = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_ms_zbjl_tlsc(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)

        timepoint = CharField(max_length=256, null=False)
        shichang = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_ss_zbjl_tlsc(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)

        timepoint = CharField(max_length=256, null=False)
        shichang = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_kj_zbjl_tlsc(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)

        timepoint = CharField(max_length=256, null=False)
        shichang = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_yl_zbjl_tlsc(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)

        timepoint = CharField(max_length=256, null=False)
        shichang = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_cy_zbjl_tlsc(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)

        timepoint = CharField(max_length=256, null=False)
        shichang = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_ty_zbjl_tlsc(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)

        timepoint = CharField(max_length=256, null=False)
        shichang = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_gx_zbjl_tlsc(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)

        timepoint = CharField(max_length=256, null=False)
        shichang = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_ly_zbjl_tlsc(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)

        timepoint = CharField(max_length=256, null=False)
        shichang = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_jk_zbjl_tlsc(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)

        timepoint = CharField(max_length=256, null=False)
        shichang = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    suffix = 'd' if current_taks == ' Daily ' else 'h'

    today_log_dir = '/root/xd_crawler/log/%s' % today_date
    if not os.path.exists(today_log_dir):
        os.mkdir(today_log_dir)
    logging.basicConfig(format='%(message)s', filename=today_log_dir + '/zbjl_tlsc_%s_%s.log'%(type,suffix), level=logging.INFO)

    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logging.info("--------------------Uncaught Exception--------------------",exc_info=(exc_type, exc_value, exc_traceback))
    sys.excepthook = handle_exception

    today_tlsc_count = 0

    if current_taks == ' Daily ':

        query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.time_update.startswith('%s'),~list_%s_zbjl.time_update.endswith('History'))" % (type, type, today_date, type)

    elif current_taks == 'History':

        query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.time_update.startswith('%s'),list_%s_zbjl.time_update.endswith('History'))" % (type, type, today_date, type)

    for one_record in eval(query_cmd):

        webcast_id = one_record.url_zbjl.split('/')[-1]

        userAvgDurationNew_url = 'https://gw.newrank.cn/api/douyin-webcastdetail/xdnphb/nr/app/douyin/webcastdetail/detail/userAvgDurationNew'

        createTime = one_record.livestraming_time
        finishTime = calc_next_time(one_record.livestraming_time, one_record.duration)

        post_data = {
            "roomId": webcast_id,
            "createTime": createTime,
            "finishTime": finishTime,
            "startTime": "",
            "endTime": ""
         }

        current_node_time = calc_next_time(createTime, 1)
        count = 0
        while time_comparison(current_node_time, finishTime):

            endTime = calc_next_time(current_node_time, 1)
            if not time_comparison(endTime, finishTime):
                endTime = finishTime

            post_data.update({'startTime':current_node_time, 'endTime':endTime})

            Retry_times = 3
            continue_next_flag = False
            while 1:
                try:
                    rsp = requests.post(userAvgDurationNew_url, headers=headers, data=json.dumps(post_data))
                    data = str(json.loads(rsp.text).get('data'))
                except:
                    Retry_times -= 1
                    logging.info('[*] Get zbjl_tlsc userAvgDurationNew_url failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                    if Retry_times == 0:
                        continue_next_flag = True
                        break
                    time.sleep(5)
                else:
                    break
            if continue_next_flag:
                logging.info(' '.join(['[%s]' % current_taks, type, 'zbjl_tlsc', one_record.num_zb, one_record.name_zb, webcast_id,one_record.livestraming_time, 'userAvgDurationNew_url Error at', get_current_time()]))
                continue

            # print(current_node_time, data)
            # current_node_time = calc_next_time(current_node_time, 1)

            Table_obj = eval('list_' + type + '_zbjl_tlsc' + '.create()')
            Table_obj.num_zb = one_record.num_zb
            Table_obj.id_zb = one_record.id_zb
            Table_obj.name_zb = one_record.name_zb
            Table_obj.url_zbjl = one_record.url_zbjl
            Table_obj.livestraming_time = one_record.livestraming_time

            Table_obj.timepoint = current_node_time
            Table_obj.shichang = data

            if current_taks == ' Daily ':
                Table_obj.time_update = get_current_time()
            elif current_taks == 'History':
                Table_obj.time_update = get_current_time() + ' History'
            Table_obj.save()

            count += 1
            today_tlsc_count += 1
            current_node_time = calc_next_time(current_node_time, 1)

        logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl_tlsc', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, '[ tlsc_count: %d ]'%count, 'Done at', get_current_time()]))

    logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl_tlsc', '[ today_tlsc_count: %d ]' % today_tlsc_count, 'Done at', get_current_time()]))
    logging.info('-'*100)
    db1.close()

Entry_list = {
    ' Daily ': True,
    'History': True
}
pool = Pool(6)
for current_taks in Entry_list:
    for type in input_type:
        pool.apply_async(run_crawler_task, args=(type, current_taks))
pool.close()
pool.join()
pool.terminate()