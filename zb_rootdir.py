# coding=utf-8
# no need update
# 只是最初建立，以后不更新

import requests
import json
import time
import datetime
import pandas
from DB import *

today_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
last_month_last_day = (datetime.date(datetime.date.today().year,datetime.date.today().month,1)-datetime.timedelta(1)).strftime("%Y-%m-%d")

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

    data_sheet = pandas.read_excel('./download_excel/%s.xlsx'%type, sheet_name='基础数据' )
    douyin_id_list = list(data_sheet['抖音号ID'])
    douyin_name_list = list(data_sheet['抖音号名称'])
    douyin_identify_list = list(data_sheet['认证'])

    if type == 'kj':
        douyin_id_list = douyin_id_list[:100]

    for id_zb in douyin_id_list:

        post_data = {
            'keyword': id_zb
        }
        id_search_error_tag = False
        name_search_error_tag = False
        #identify_search_error_tag = False
        while 1:
            try:
                searchAccount_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/searchAccountList'
                rsp = requests.post(searchAccount_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data').get('list')
                if len(data) == 0 and (not id_search_error_tag):
                    id_search_error_tag = True
                    raise Exception
                if len(data) == 0 and (not name_search_error_tag):
                    name_search_error_tag = True
                    raise Exception
            except:
                if id_search_error_tag and (not name_search_error_tag):
                    search_name = douyin_name_list[douyin_id_list.index(id_zb)]
                    if '（' in search_name:
                        search_name = search_name.split('（')[0]
                    if '?' in search_name:
                        search_name = search_name.split('?')[0]
                    post_data.update({'keyword': search_name})

                if name_search_error_tag:
                    search_identify = douyin_identify_list[douyin_id_list.index(id_zb)]
                    post_data.update({'keyword': search_identify})

                print('[*] Get zb_rootdir failed. type:%s id_zb:%s id_search_error_tag:%s name_search_error_tag:%s' % (type,id_zb,id_search_error_tag,name_search_error_tag))
                time.sleep(5)
            else:
                break

        if len(data) > 1:
            max_followers = max([int(i.get('mplatform_followers_count')) for i in data])
            for i in data:
                if int(i.get('mplatform_followers_count')) == max_followers:
                    one_data = i
        else:
            one_data = data[0]

        num_zb = str(douyin_id_list.index(id_zb) + 1) + type
        name_zb = douyin_name_list[douyin_id_list.index(id_zb)]
        url_zb = 'https://xd.newrank.cn/tiktok/detail/latest/%s' % one_data.get('uid')

        eval('list_' + type + '.create(num_zb=num_zb, id_zb="", name_zb=name_zb, url_zb=url_zb)')

        print("[+] %s %s Done." % (num_zb, name_zb))
    print("-" * 50)




'''for type in input_type:

    zb_index = 1
    post_data = {
        'account_type': type_list[type],
        'date_type': "2",
        'rank_date': last_month_last_day,
        'size': '50',
        'start': ''
    }

    for page in range(1, 5):

        post_data.update({'start': page})

        while 1:
            try:
                saleRank_search_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/webcastSalesRank'
                rsp = requests.post(saleRank_search_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data').get('list')
            except:
                print('[*] Get zb rootdir type:%s page:%d failed...Retrying..'%(type,page))
                time.sleep(5)
            else:
                break

        for item in data:
            num_zb = '{index}{type}'.format(index=zb_index, type=type)
            id_zb = item.get('unique_id')
            name_zb = item.get('user_nickname')
            url_zb = 'https://xd.newrank.cn/tiktok/detail/latest/%s' % item.get('uid')

            #single_list.append([num_zb,id_zb,name_zb,url_zb])
            eval('list_' + type + '.create(num_zb=num_zb, id_zb=id_zb, name_zb=name_zb, url_zb=url_zb)')
            zb_index += 1

        print("[+] %s page %d Done."%(type,page))
    print("[+] %s page %d Done." % (type, page))'''