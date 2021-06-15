# coding=utf-8
# 在5月31日抓全从1月31日到5月30日（累计4个月的）作品url的相关数据
# 从6月1日期开始每日更新，每天抓取昨天的作品url等数据（如6月1日抓取5月31日的）

import requests
import json
import datetime
import time
from DB import *

def get_current_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

today_date = datetime.datetime.now().strftime("%Y-%m-%d")
lastday_date = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
first_crawl_date = (datetime.datetime.now()+datetime.timedelta(days=-121)).strftime("%Y-%m-%d")
FIRST_RUN_DATE = '2021-06-01'

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

    for one_record in eval('list_' + type + '.select()'):

        uid = one_record.url_zb.split('/')[-1]
        aweme_id_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/detail/aweme'

        # if today_date == '2021-05-31':
        #     post_data.update(({'create_time_start':first_crawl_date}))
        #     post_data.update(({'create_time_end': today_date}))
        # else:
        #     post_data.update(({'create_time_start': lastday_date}))
        #     post_data.update(({'create_time_end': lastday_date}))

        # while 1:
        #     try:
        #         rsp = requests.post(aweme_id_url, headers=headers, data=json.dumps(post_data))
        #         data = json.loads(rsp.text).get('data')
        #         count = data.get('count')
        #         end_page = int(count / 100) + 1 if count % 100 != 0 else int(count / 100)
        #     except:
        #         print('[*] Get zb_zplb aweme_id_url count failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        #         time.sleep(5)
        #     else:
        #         break

        # aweme_id_list = []
        # for page in range(1,end_page+1):
        #     while 1:
        #         try:
        #             post_data.update({'start': page})
        #             rsp = requests.post(aweme_id_url, headers=headers, data=json.dumps(post_data))
        #             data = json.loads(rsp.text).get('data')
        #             data_list = data.get('list') if data else []
        #         except:
        #             print('[*] Get zb_zplb aweme_id_url data failed. type:%s, num_zb:%s, url_zb:%s at %s' % (
        #             type, one_record.num_zb, one_record.url_zb, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        #             time.sleep(5)
        #         else:
        #             break
        #
        #     for item in data_list:
        #         aweme_id_list.append(item.get('aweme_id'))


        if today_date == FIRST_RUN_DATE:
            update_date = datetime.datetime.strptime(first_crawl_date, "%Y-%m-%d")
        else:
            update_date = datetime.datetime.strptime(lastday_date, "%Y-%m-%d")

        aweme_id_list = []
        finish_get_awemeid_flag = True
        set_start = (datetime.datetime.now() + datetime.timedelta(days=0)).strftime("%Y-%m-%d")
        while finish_get_awemeid_flag:

            post_data = {
                "keyword": "",
                "uid": uid,
                "date_type": "",
                "sort": "create_time",
                "is_seed": "0",
                "is_promotion": "0",
                "is_del": "0",
                "micro_app_exists": "0",
                "size": 100,
                "start": 1,
                "create_time_start": '',
                "create_time_end": ''
            }

            if today_date == FIRST_RUN_DATE:
                set_end = (datetime.datetime.strptime(set_start, "%Y-%m-%d") + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
                set_start = (datetime.datetime.strptime(set_end, "%Y-%m-%d") + datetime.timedelta(days=-25)).strftime("%Y-%m-%d")
            else:
                set_end = lastday_date
                set_start = lastday_date

            post_data.update(({'create_time_start':set_start}))
            post_data.update(({'create_time_end': set_end}))
            page = 1
            new_time_period = True
            while 1:
                try:
                    post_data.update({'start': page})
                    rsp = requests.post(aweme_id_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data')
                    data_list = data.get('list')

                    if (data_list == []) and new_time_period:
                        #当前新时间周期里已经没有了新数据，结束时间周期搜索循环
                        finish_get_awemeid_flag = False
                        break

                    if (data_list == []) and (page*100>data.get('count')):
                        #翻页结束,进入下一个时间周期
                        break

                    for item in data_list:

                        zp_create_time = datetime.datetime.strptime(item.get('create_time').split(' ')[0], "%Y-%m-%d")

                        if zp_create_time < update_date:
                            finish_get_awemeid_flag = False
                            break
                        else:
                            aweme_id_list.append(item.get('aweme_id'))

                    if set_end == set_start:
                        finish_get_awemeid_flag = False
                        break
                    new_time_period = False
                    page += 1
                except:
                    print('[*] Get zb_zplb aweme_id_list failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, get_current_time()))
                    time.sleep(5)

        aweme_Detail_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/aweme/awemeDetail/getAwemeDetailLeft'
        for aweme_id in aweme_id_list:

            url_works = "https://xd.newrank.cn/material/detail/comment/%s" % aweme_id

            query_cmd = "list_%s_zplb.select().where(list_%s_zplb.url_works=='%s',list_%s_zplb.time_update.startswith('%s'))" % (type, type, url_works, type, today_date)
            if eval(query_cmd):
                continue

            while 1:
                try:
                    rsp = requests.post(aweme_Detail_url, headers=headers, data=json.dumps({"aweme_id": aweme_id}))
                    data_ori = json.loads(rsp.text)
                    data = data_ori.get('data')
                except:
                    print('[*] Get zb_zplb aweme_Detail_url failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, get_current_time()))
                    time.sleep(5)
                else:
                    break

            if data_ori.get('code') == 4014 and data == None:

                print('[*] %s'%data_ori.get('msg'),aweme_id,'at',time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

                Table_obj = eval('list_' + type + '_zplb' + '.create()')
                Table_obj.num_zb = one_record.num_zb
                Table_obj.id_zb = one_record.id_zb
                Table_obj.name_zb = one_record.name_zb
                Table_obj.url_zb = one_record.url_zb
                Table_obj.url_works = url_works
                Table_obj.time_update = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                Table_obj.save()

                continue



            Table_obj = eval('list_' + type + '_zplb' + '.create()')

            Table_obj.num_zb = one_record.num_zb
            Table_obj.id_zb = one_record.id_zb
            Table_obj.name_zb = one_record.name_zb
            Table_obj.url_zb = one_record.url_zb
            Table_obj.url_works = url_works

            Table_obj.content = data.get('aweme_desc')
            Table_obj.duration = data.get('duration')
            Table_obj.time_release = data.get('create_time')
            Table_obj.music = data.get('music_info').get('title') if data.get('music_info') else '--'
            Table_obj.daoju = data.get('sticker_detail').get('name') if data.get('sticker_detail') else '--'

            if data.get('is_promotion') != "0":
                Table_obj.product = data.get('promotion_info')[0].get('title')
                Table_obj.sply = data.get('promotion_info')[0].get('goods_source')
                Table_obj.time_update = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                #product_url = 'https://xd.newrank.cn/goodsDetail/basic/%s' % (data.get('product_id'))
            else:
                Table_obj.product, Table_obj.sply = ["--"] * 2
                Table_obj.time_update = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

            Table_obj.save()

        #print('|'.join([content,duration,time_release,music,daoju,product,sply]))
        print('[+]',type,'zb_zplb',one_record.num_zb,one_record.name_zb,'zp amount:%d'%len(aweme_id_list),'Done at', get_current_time())