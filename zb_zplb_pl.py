# coding=utf-8
# 从6月1日期开始每日更新，每天抓取是4个月前记录的作品url的数据（如6月1日抓取的是1月31日的）即向前推121天

import requests
import json
import datetime
import time
from DB import *

def get_current_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

update_date = (datetime.datetime.now()+datetime.timedelta(days=-121)).strftime("%Y-%m-%d")

type_list = {
    'ms':'美食','ss':'时尚','kj':'科技',
    'yl':'娱乐','gx':'搞笑','cy':'才艺',
    'qy':'企业',
}

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

for type in input_type:

    query_cmd = "list_%s_zplb.select().where(list_%s_zplb.time_release.startswith('%s'))" % (type,type,update_date)

    for one_record in eval(query_cmd):

        aweme_id = one_record.url_works.split('/')[-1]
        aweme_comment_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/aweme/awemeDetail/listAwemeComment'
        post_data = {
            "aweme_id": aweme_id,
            "size": 100,
            "start": 1
        }
        while 1:
            try:
                rsp = requests.post(aweme_comment_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data')
                count = data.get('count')
                end_page = int(count / 100) + 1 if count % 100 != 0 else int(count / 100)
            except:
                print('[*] Get zb_zplb_pl aweme_comment_url count failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, get_current_time()))
                time.sleep(5)
            else:
                break


        for page in range(1,end_page+1):

            while 1:
                try:
                    post_data.update({'start': page})
                    rsp = requests.post(aweme_comment_url, headers=headers, data=json.dumps(post_data))
                    data_list = json.loads(rsp.text).get('data').get('list')
                except:
                    print('[*] Get zb_zplb_pl aweme_id_url data failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, get_current_time()))
                    time.sleep(5)
                else:
                    break

            for item in data_list:

                Table_obj = eval('list_' + type + '_zplb_pl' + '.create()')

                Table_obj.num_zb = one_record.num_zb
                Table_obj.id_zb = one_record.id_zb
                Table_obj.name_zb = one_record.name_zb
                Table_obj.url_zb = one_record.url_zb
                Table_obj.url_works = one_record.url_works

                Table_obj.reviewer = item.get('user_imfor')[0].get('nickname') if item.get('user_imfor') else '--'
                Table_obj.xin = str(item.get('digg_count'))
                Table_obj.comment = item.get('text')
                Table_obj.time_comment = item.get('create_time')
                Table_obj.time_update = get_current_time()

                Table_obj.save()

        print('[+]', type, 'zb_zplb_pl', one_record.num_zb, one_record.name_zb, aweme_id, "Done %s's update at"%update_date, get_current_time())
