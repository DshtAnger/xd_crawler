# coding=utf-8
# 首日启动,抓取过去累计4个月(121天)的作品url相关数据(如5月31日抓全从1月31日到5月30日)
# 次日启动,每天抓取昨天的作品url数据(如6月1日抓取5月31日的)

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

    for one_record in eval('list_' + type + '.select()'):

        uid = one_record.url_zb.split('/')[-1]
        aweme_id_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/detail/aweme'

        if today_date == FIRST_RUN_DATE:
            update_date = datetime.datetime.strptime(first_crawl_date, "%Y-%m-%d")
        else:
            update_date = datetime.datetime.strptime(lastday_date, "%Y-%m-%d")

        zp_count = 0
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

            Retry_times = 10
            continue_next_flag = False
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

                    for aweme_obj in data_list:

                        zp_create_time = datetime.datetime.strptime(aweme_obj.get('create_time').split(' ')[0], "%Y-%m-%d")

                        if zp_create_time < update_date:
                            finish_get_awemeid_flag = False
                            break
                        else:
                            aweme_id = aweme_obj.get('aweme_id')
                            url_works = "https://xd.newrank.cn/material/detail/comment/%s" % aweme_id

                            query_cmd = "list_%s_zplb.select().where(list_%s_zplb.url_works=='%s',list_%s_zplb.time_update.startswith('%s'))" % (type, type, url_works, type, today_date)
                            if eval(query_cmd):
                                continue

                            Table_obj = eval('list_' + type + '_zplb' + '.create()')

                            Table_obj.num_zb = one_record.num_zb
                            Table_obj.id_zb = one_record.id_zb
                            Table_obj.name_zb = one_record.name_zb
                            Table_obj.url_zb = one_record.url_zb
                            Table_obj.url_works = url_works

                            Table_obj.content = aweme_obj.get('aweme_desc')
                            Table_obj.duration = aweme_obj.get('duration')
                            Table_obj.time_release = aweme_obj.get('create_time')
                            Table_obj.music = aweme_obj.get('music_info').get('title') if aweme_obj.get('music_info') else '--'

                            Table_obj.time_update = get_current_time()
                            Table_obj.save()

                            zp_count += 1

                    if set_end == set_start:
                        finish_get_awemeid_flag = False
                        break
                    new_time_period = False
                    page += 1
                except:
                    Retry_times -= 1
                    print('[*] Get zb_zplb aweme_id_url failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, get_current_time()))
                    if Retry_times == 0:
                        continue_next_flag = True
                        break
                    time.sleep(5)
                else:
                    break
            if continue_next_flag:
                Table_obj.time_update = '[%s] aweme_id_url Error has occurred' % get_current_time()
                Table_obj.save()
                continue

        print('[+]', type, 'zb_zplb', one_record.num_zb, one_record.name_zb, '[ zp amount: %d ]'%zp_count, 'Done at', get_current_time())