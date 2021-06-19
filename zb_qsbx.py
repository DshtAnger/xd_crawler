# coding=utf-8
# no need update
# 5月31日抓取相应近90天的趋势数据,以后不更新

import requests
import json
import datetime
import time
import os,sys
import logging
from DB import *

def get_current_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

today_date = datetime.datetime.now().strftime("%Y-%m-%d")
today_log_dir = '/root/xd_crawler/log/%s' % today_date
if not os.path.exists(today_log_dir):
    os.mkdir(today_log_dir)
logging.basicConfig(format='%(message)s',filename=today_log_dir + '/zb_qsbx.log', level=logging.INFO)
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.info("--------------------Uncaught Exception--------------------",exc_info=(exc_type, exc_value, exc_traceback))
sys.excepthook = handle_exception

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

    query_cmd_fans = eval('list_%s_qsbx_fans.select()' % type)
    query_cmd_supports =  eval('list_%s_qsbx_supports.select()' % type)
    query_cmd_works =  eval('list_%s_qsbx_works.select()' % type)

    if (len(query_cmd_fans) != 0) or (len(query_cmd_supports) != 0) or (len(query_cmd_works) != 0):
        logging.info(' '.join(['[+]', type, 'zb_qsbx', 'Do not need to update at', get_current_time()]))
        continue

    today_zb_qsbx_count = 0
    for one_record in eval('list_' + type + '.select()'):

        uid = one_record.url_zb.split('/')[-1]
        trend_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/detail/trend'
        post_data = {
            "uid":uid,
            "dateType":3
        }

        Retry_times = 10
        continue_next_flag = False
        while 1:
            try:
                rsp = requests.post(trend_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data')
                Retry_times -= 1
            except:
                logging.info('[*] Get zb_qsbx trend_url failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, get_current_time()))
                if Retry_times == 0:
                    continue_next_flag = True
                    break
                time.sleep(5)
            else:
                break
        if continue_next_flag:
            Table_obj_fans = eval('list_' + type + '_qsbx_fans' + '.create()')
            Table_obj_supports = eval('list_' + type + '_qsbx_supports' + '.create()')
            Table_obj_works = eval('list_' + type + '_qsbx_works' + '.create()')

            Table_obj_fans.time_update = '[%s] trend_url Error has occurred' % get_current_time()
            Table_obj_supports.time_update = '[%s] trend_url Error has occurred' % get_current_time()
            Table_obj_works.time_update = '[%s] trend_url Error has occurred' % get_current_time()

            Table_obj_fans.save()
            Table_obj_supports.save()
            Table_obj_works.save()

            continue

        for item in data:
            Table_obj_fans = eval('list_' + type + '_qsbx_fans' + '.create()')
            Table_obj_supports = eval('list_' + type + '_qsbx_supports' + '.create()')
            Table_obj_works = eval('list_' + type + '_qsbx_works' + '.create()')

            Table_obj_fans.num_zb = one_record.num_zb
            Table_obj_fans.id_zb = one_record.id_zb
            Table_obj_fans.name_zb = one_record.name_zb
            Table_obj_fans.url_zb = one_record.url_zb
            Table_obj_fans.fans_time = item.get('rank_date')
            Table_obj_fans.fans_zl = item.get('mplatform_followers_count')
            Table_obj_fans.fbzp = item.get('aweme_count_today') if item.get('aweme_count_today') else '--'
            Table_obj_fans.zb = item.get('webcast_count_today') if item.get('webcast_count_today') else '--'
            Table_obj_fans.time_update = get_current_time()

            Table_obj_supports.num_zb = one_record.num_zb
            Table_obj_supports.id_zb = one_record.id_zb
            Table_obj_supports.name_zb = one_record.name_zb
            Table_obj_supports.url_zb = one_record.url_zb
            Table_obj_supports.supports_time = item.get('rank_date') if item.get('rank_date') else '--'
            Table_obj_supports.support_zl = item.get('total_favorited') if item.get('total_favorited') else '--'
            Table_obj_supports.time_update = get_current_time()

            Table_obj_works.num_zb = one_record.num_zb
            Table_obj_works.id_zb = one_record.id_zb
            Table_obj_works.name_zb = one_record.name_zb
            Table_obj_works.url_zb = one_record.url_zb
            Table_obj_works.works_time = item.get('rank_date') if item.get('rank_date') else '--'
            Table_obj_works.works_zl = item.get('aweme_count') if item.get('aweme_count') else '--'
            Table_obj_works.time_update = get_current_time()

            Table_obj_fans.save()
            Table_obj_supports.save()
            Table_obj_works.save()

            today_zb_qsbx_count += 1

        logging.info(' '.join(['[+]', type, 'zb_qsbx', one_record.num_zb, one_record.name_zb, 'Done at', get_current_time()]))
    else:
        logging.info(' '.join(['[+]', type, 'zb_qsbx', '[ today_zb_qsbx_count: %d ]'%today_zb_qsbx_count, 'Done at', get_current_time()]))
        logging.info('-' * 100)