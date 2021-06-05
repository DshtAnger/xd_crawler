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
FIRST_RUN_DATE = '2021-06-06'

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
        set_start = (datetime.datetime.now() + datetime.timedelta(days=0)).strftime("%Y-%m-%d")
        Time_period_loop = True
        while Time_period_loop:
            # 时间段循环
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

            Retry_times = 10
            Turn_page_loop = True
            while Turn_page_loop:
                # 翻页循环
                try:
                    post_data.update({'start': page})
                    rsp = requests.post(aweme_id_url, headers=headers, data=json.dumps(post_data))
                    data_ori = json.loads(rsp.text)
                    data = data_ori.get('data')

                    if data_ori.get('code') == 4014 and data == None:
                        import os, re
                        error_msg = '|'.join(re.search(r'<div class="xd_noauth_wrap"><div class="xd_noauth_title">(.*)</div><div class="xd_noauth_subtitle">(.*)<span class="xd_noauth_count">(.*)</span>(.*)</div></div>',data_ori.get('msg')).groups())
                        print('[*] %s' % error_msg, one_record.num_zb, one_record.name_zb, 'at', get_current_time())
                        os._exit(1)
                    else:
                         data_list = data.get('list')
                    #'{\n\t"msg":"<div class=\\"xd_noauth_wrap\\"><div class=\\"xd_noauth_title\\">今日访问次数已达上限</div><div class=\\"xd_noauth_subtitle\\">您当前为超级全家桶，该页面访问次数为<span class=\\"xd_noauth_count\\">8000</span>次/天</div></div>",\n\t"data":null,\n\t"code":4014\n}'

                    if (data_list == []) and Turn_page_loop:
                        # 上一时间段内翻页到底后，当前新时间段里也没有数据，则意味着数据抓取完毕，时间段不用再向前，结束间整个搜索循环
                        Time_period_loop = False
                        break

                    for aweme_obj in data_list:

                        zp_create_time = datetime.datetime.strptime(aweme_obj.get('create_time').split(' ')[0], "%Y-%m-%d")

                        if zp_create_time < update_date:
                            Turn_page_loop = False
                            Time_period_loop = False
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

                    if ( len(data_list) <= 100 ) and ( page*100 >= data.get('count') ):
                        # 多页情景下,当前时间段内没有新数据了,不再翻页,进入下一个时间段
                        # 单页情景下,当前时间段内只有不到100个数据或者刚好100个数据时,依然适用
                        if set_end == set_start:
                            # 日更场景,昨天的数据翻页完,退出时间段循环
                            Time_period_loop = False
                        break
                    page += 1
                except:
                    Retry_times -= 1
                    print('[*] Get zb_zplb aweme_id_url failed. type:%s, num_zb:%s, url_zb:%s, time_period:%s-%s, page:%d, Retry occurred at %s' % (type, one_record.num_zb, one_record.url_zb, set_start, set_end, page, get_current_time()))

                    if Retry_times == 0:
                        Turn_page_loop = False
                        Time_period_loop = False
                        print('[*]', type, 'zb_zplb', one_record.num_zb, one_record.name_zb, one_record.url_zb, 'time_period:%s-%s'%(set_start,set_end), 'page:%d'%page, 'Severe Error has occurred to continue next zb at', get_current_time())

                    time.sleep(5)

        print('[+]', type, 'zb_zplb', one_record.num_zb, one_record.name_zb, '[ zp amount: %d ]'%zp_count, 'Done at', get_current_time())