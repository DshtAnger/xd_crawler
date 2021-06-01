# coding=utf-8
# 从6月1日期开始每日更新，每天抓取昨天的数据（如6月1日抓取5月31日的）

import requests
import websocket
import json
import datetime
import time
from DB import *


today_date = datetime.datetime.now().strftime("%Y-%m-%d")
lastday_date = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
first_crawl_date = (datetime.datetime.now()+datetime.timedelta(days=-121)).strftime("%Y-%m-%d")

type_list = {
    'ms':'美食','ss':'时尚','kj':'科技',
    'yl':'娱乐','gx':'搞笑','cy':'才艺',
    'qy':'企业',
}

with open('./cookie','r') as f:
    cookie = f.read().strip()
with open('./token','r') as f:
    token = f.read().strip()
with open('./type','r') as f:
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

for type in input_type:

    query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.livestraming_time.startswith('%s'))" % (type,type,lastday_date)

    for one_record in eval(query_cmd):

        webcast_id = one_record.url_zbjl.split('/')[-1]
        comment_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/webcastMessageList'
        post_data = {
            "room_id": webcast_id,
            "size": 100,
            "start": 1,
        }

        while 1:
            try:
                rsp = requests.post(comment_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data')
                count = data.get('count') if data else -1
                end_page = int(count / 100) + 1 if count % 100 != 0 else int(count / 100)
            except:
                print(
                    '[*] Get zbjl_pl comment_url count failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl,time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                time.sleep(5)
            else:
                break
        if count == -1:
            print('[No_pl_data]', type, 'zbjl_pl', one_record.num_zb, one_record.name_zb, webcast_id,one_record.livestraming_time, 'Done at', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            continue

        for page in range(1, end_page + 1):

            while 1:
                try:
                    post_data.update({'start': page})
                    rsp = requests.post(comment_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data')
                    data_list = data.get('list') if data else []
                except:
                    print('[*] Get zbjl_pl comment_url data failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl,time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                    time.sleep(5)
                else:
                    break

            for item in data_list:

                Table_obj = eval('list_' + type + '_zbjl_pl' + '.create()')

                Table_obj.num_zb = one_record.num_zb
                Table_obj.id_zb = one_record.id_zb
                Table_obj.name_zb = one_record.name_zb
                Table_obj.url_zbjl = one_record.url_zbjl
                Table_obj.livestraming_time = one_record.livestraming_time

                Table_obj.review_time = item.get('create_time')
                Table_obj.author = item.get('user').get('nickname')
                Table_obj.content = item.get('content')
                Table_obj.time_update = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                Table_obj.save()

        print('[+]', type, 'zbjl_pl', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, 'Done at',time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))