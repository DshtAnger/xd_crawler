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
        aweme_userinfo_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/aweme/awemeDetail/getAwemeUserInfo'
        post_data = {
            "aweme_id": aweme_id
        }

        while 1:
            try:
                rsp = requests.post(aweme_userinfo_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data')
            except:
                print('[*] Get zb_zplb_cyzhx failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, get_current_time()))
                time.sleep(5)
            else:
                break

        Table_obj = eval('list_' + type + '_zplb_cyzhx' + '.create()')

        Table_obj.num_zb = one_record.num_zb
        Table_obj.id_zb = one_record.id_zb
        Table_obj.name_zb = one_record.name_zb
        Table_obj.url_zb = one_record.url_zb
        Table_obj.url_works = one_record.url_works

        if not data:
            Table_obj.female,Table_obj.male,Table_obj.eighteen, Table_obj.eighteentotwentythree, Table_obj.twentyfourtothirty, Table_obj.thirtyonetofourty, Table_obj.fourtyonetofifty, Table_obj.fifty = ['--'] * 8
            Table_obj.time_update = get_current_time()
            Table_obj.save()
            continue

        Table_obj.female, Table_obj.male = ['--'] * 2
        for gender in data.get('user_gender'):
            if gender.get('key') == "女":
                Table_obj.female = gender.get('rate')
            if gender.get('key') == "男":
                Table_obj.male = gender.get('rate')

        Table_obj.eighteen, Table_obj.eighteentotwentythree, Table_obj.twentyfourtothirty, Table_obj.thirtyonetofourty, Table_obj.fourtyonetofifty, Table_obj.fifty = ['0'] * 6
        for age in data.get('user_age'):
            if age.get('key') == '<18':
                Table_obj.eighteen = age.get('rate')
            if age.get('key') == '18-23':
                Table_obj.eighteentotwentythree = age.get('rate')
            if age.get('key') == '24-30':
                Table_obj.twentyfourtothirty = age.get('rate')
            if age.get('key') == '31-40':
                Table_obj.thirtyonetofourty = age.get('rate')
            if age.get('key') == '41-50':
                Table_obj.fourtyonetofifty = age.get('rate')
            if age.get('key') == '>50':
                Table_obj.fifty = age.get('rate')

        Table_obj.time_update = get_current_time()

        Table_obj.save()

        print('[+]', type, 'zb_zplb_cyzhx', one_record.num_zb, one_record.name_zb, aweme_id, "Done %s's update at"%update_date, get_current_time())
