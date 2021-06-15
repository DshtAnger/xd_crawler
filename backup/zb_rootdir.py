# coding=utf-8

import requests
import json
import time
from DB import *

type_list = {
    'ms':'美食','ss':'时尚','科技':'kj',
    'yl':'娱乐','gx':'搞笑','cy':'才艺',
    'qy':'企业',
}

# '游戏': 'yx', '家居': 'jj', '萌宠': 'mc',
# '旅游': 'ly', '健康': 'jk', '汽车': 'qc', '社会': 'sh'

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

    single_list = []
    zb_index = 1
    for page in range(1, 10):

        douyin_search_data = {
            "input": {"keyword": "", "type": ""},
            "xd_tags": [],
            "type": None,
            "nr_range_list": [],
            "verify": "",
            "contact": "",
            "mcn": "0",
            "with_fusion_shop_entry": "",
            "is_live": "",
            "account_info": {"province": "", "city": "", "gender": "", "constellation_name": "", "age_range": ""},
            "data_performance": {"favorited_range_list": [], "follower_range_list": []},
            "relate_goods": {"goods_name": "", "goods_cate1": "", "goods_cate2": "", "price_range": "",
                             "goods_sales_range": "",
                             "visitor_count_range": "", "goods_source": "", "price": {"gte": "", "lt": ""}},
            "fans_info": {"province": "", "city": "", "gender": "", "age_range": "", "constellation_name": ""},
            "sort": "newrank_index",
            "size": 50,
            "start": None,
            "is_claim": ""
        }

        douyin_search_data.update({'type':type})
        douyin_search_data.update({'start': page})

        while 1:
            try:
                pt_search_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/accountSearch'
                rsp = requests.post(pt_search_url, headers=headers, data=json.dumps(douyin_search_data))
                data = json.loads(rsp.text).get('data').get('list')
            except:
                print('[*] Get ptzb rootdir type:%s page:%d faile...Retrying..'%(type,page))
                time.sleep(5)
            else:
                break

        for item in data:
            num_zb = '{index}pt{type}'.format(index=zb_index,type=search_type[type])
            id_zb = item.get('account')
            name_zb = item.get('nickname')
            url_zb = 'https://xd.newrank.cn/tiktok/detail/latest/%s'%item.get('uid')

            #single_list.append([num_zb,id_zb,name_zb,url_zb])
            eval('list_' + search_type[type] + '.create(num_zb=num_zb, id_zb=id_zb, name_zb=name_zb, url_zb=url_zb)')
            zb_index += 1

        print("[+] pt%s page %d Done."%(type,page))

    #pt_list.append(single_list)



#mcn_list = []
for type in search_type:

    single_list = []
    zb_index = 1
    for page in range(1, 7):
        mcn_search_data = {
            "input": {"keyword": "", "type": ""},
            "mcn_city": "",
            "xd_tags": [],
            "type": None,
            "nr_range_list": [],
            "verify": "",
            "contact": "",
            "mcn": "1",
            "with_fusion_shop_entry": "",
            "is_live": "",
            "relate_mcn": {"mcn_follower_tag": [], "mcn_user_tag": []},
            "account_info": {"province": "", "city": "", "gender": "", "constellation_name": "", "age_range": ""},
            "data_performance": {"favorited_range_list": [], "follower_range_list": []},
            "relate_goods": {"goods_name": "", "goods_cate1": "", "goods_cate2": "", "price_range": "",
                             "goods_sales_range": "", "visitor_count_range": "", "goods_source": "",
                             "price": {"gte": "", "lt": ""}},
            "fans_info": {"province": "", "city": "", "gender": "", "age_range": "", "constellation_name": ""},
            "sort": "newrank_index",
            "size": 50,
            "start": None
        }
        mcn_search_data.update({'type':type})
        mcn_search_data.update({'start': page})

        while 1:
            try:
                mcn_search_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/mcnAccountSearch'
                rsp = requests.post(mcn_search_url, headers=headers, data=json.dumps(mcn_search_data))
                data = json.loads(rsp.text).get('data').get('list')
            except:
                print('[*] Get mcnzb rootdir type:%s page:%d faile...Retrying..'%(type,page))
                time.sleep(5)
            else:
                break

        for item in data:
            num_zb = '{index}mcn{type}'.format(index=zb_index,type=search_type[type])
            id_zb = item.get('account')
            name_zb = item.get('nickname')
            url_zb = 'https://xd.newrank.cn/tiktok/detail/latest/%s'%item.get('uid')

            #single_list.append([num_zb,id_zb,name_zb,url_zb])
            eval('list_' + search_type[type] + '.create(num_zb=num_zb, id_zb=id_zb, name_zb=name_zb, url_zb=url_zb)')
            zb_index += 1

        print("[+] mcn%s page %d Done."%(type,page))

    #mcn_list.append(single_list)
