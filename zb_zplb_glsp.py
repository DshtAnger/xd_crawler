# coding=utf-8
# 从6月1日期开始每日更新，每天抓取是4个月前记录的作品url的数据（如6月1日抓取的是1月31日的）即向前推121天

import requests
import json
import datetime
import time
from DB import *

update_date = (datetime.datetime.now()+datetime.timedelta(days=-121)).strftime("%Y-%m-%d")

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

    query_cmd = "list_%s_zplb.select().where(list_%s_zplb.time_release.startswith('%s'))" % (type,type,update_date)

    for one_record in eval(query_cmd):

        aweme_id = one_record.url_works.split('/')[-1]
        aweme_product_goodList_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/good/awemeProduct/goodList'
        post_data = {
            "awemeId": aweme_id
        }
        while 1:
            try:
                rsp = requests.post(aweme_product_goodList_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data')
            except:
                print('[*] Get zplb_glsp aweme_product_goodList_url faile. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                time.sleep(5)
            else:
                break

        if data:
            data = data[0]

            product	= data.get('title')
            price = str(data.get('latestPrice')) if data.get('latestPrice') else '--'

            while 1:
                try:
                    aweme_product_saleInfo_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/good/awemeProduct/saleInfo'
                    rsp = requests.post(aweme_product_saleInfo_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data').get('salesTrend')
                except:
                    print('[*] Get zb_zplb_glsp aweme_product_saleInfo_url failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                    time.sleep(5)
                else:
                    break

            for item in data:

                Table_obj = eval('list_' + type + '_zplb_glsp' + '.create()')

                Table_obj.num_zb = one_record.num_zb
                Table_obj.id_zb = one_record.id_zb
                Table_obj.name_zb = one_record.name_zb
                Table_obj.url_zb = one_record.url_zb
                Table_obj.url_works = one_record.url_works

                Table_obj.product = product
                Table_obj.sales = str(item.get('sales'))
                Table_obj.time_sales = item.get('rankDate')
                Table_obj.price = price
                Table_obj.time_update = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                Table_obj.save()
        else:

            Table_obj = eval('list_' + type + '_zplb_glsp' + '.create()')

            Table_obj.num_zb = one_record.num_zb
            Table_obj.id_zb = one_record.id_zb
            Table_obj.name_zb = one_record.name_zb
            Table_obj.url_zb = one_record.url_zb
            Table_obj.url_works = one_record.url_works

            Table_obj.product, Table_obj.sales, Table_obj.time_sales, Table_obj.price = ['--'] * 4
            Table_obj.time_update = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

            Table_obj.save()

        print('[+]', type, 'zb_zplb_glsp', one_record.num_zb, one_record.name_zb, aweme_id, 'Done %s update at'%update_date, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


