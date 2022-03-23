# coding=utf-8
# 首日启动,什么都不做,轮空
# 次日启动,每天抓取昨天的相应数据(如6月1日抓取5月31日的)

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
lastday_date = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
first_crawl_date = (datetime.datetime.now()+datetime.timedelta(days=-121)).strftime("%Y-%m-%d")
today_log_dir = '/root/xd_crawler/log/%s' % today_date
if not os.path.exists(today_log_dir):
    os.mkdir(today_log_dir)
logging.basicConfig(format='%(message)s',filename=today_log_dir + '/zbjl_rc.log', level=logging.INFO)
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

Entry_list = {
    ' Daily ': True,
    'History': True
}
for current_taks in Entry_list:

    for type in input_type:
        today_rc_count = 0

        if current_taks == ' Daily ':

            query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.time_update.startswith('%s'),~list_%s_zbjl.time_update.endswith('History'))"%(type, type, today_date, type)

        elif current_taks == 'History':

            query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.time_update.startswith('%s'),list_%s_zbjl.time_update.endswith('History'))"%(type, type, today_date, type)

        for one_record in eval(query_cmd):

            webcast_id = one_record.url_zbjl.split('/')[-1]

            userSourceInfo_url = 'https://gw.newrank.cn/api/douyin-webcastdetail/xdnphb/nr/app/douyin/webcastdetail/detail/userSourceInfo'
            post_data = {
                'roomId': webcast_id
            }
            Retry_times = 10
            continue_next_flag = False
            while 1:
                try:
                    rsp = requests.post(userSourceInfo_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data')
                    data_list = data.get('trend') if isinstance(data, dict) and isinstance(data.get('trend'), list) else []
                except:
                    Retry_times -= 1
                    logging.info('[*] Get zbjl_rc userSourceInfo_url failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                    if Retry_times == 0:
                        continue_next_flag = True
                        break
                    time.sleep(5)
                else:
                    break
            if continue_next_flag:
                logging.info(' '.join(['[%s]' % current_taks, type, 'zbjl_rc', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, 'userSourceInfo_url Error at', get_current_time()]))
                continue


            for item in data_list:

                crawl_time = item.get('gmtCreate')

                Table_obj_rc = eval('list_' + type + '_zbjl_rc' + '.create()')
                Table_obj_rc.num_zb = one_record.num_zb
                Table_obj_rc.id_zb = one_record.id_zb
                Table_obj_rc.name_zb = one_record.name_zb
                Table_obj_rc.url_zbjl = one_record.url_zbjl
                Table_obj_rc.livestraming_time = one_record.livestraming_time
                Table_obj_rc.watching_time = crawl_time

                #Table_obj_rc.leijirenci = item.get('stats_total_user')#现版本已无数据
                Table_obj_rc.guanzhu = str(item.get('followCount'))
                Table_obj_rc.shipintuijian = str(item.get('videoDetailCount'))
                Table_obj_rc.zhiboguangchang = str(item.get('otherCount'))

                if current_taks == ' Daily ':
                    Table_obj_rc.time_update = get_current_time()
                elif current_taks == 'History':
                    Table_obj_rc.time_update = get_current_time() + ' History'
                Table_obj_rc.save()


            today_rc_count += len(data_list)
            logging.info(' '.join(['[%s]' % current_taks, type, 'zbjl_rc', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, '[ zbjl_rc_count: %d ]' % len(data_list), 'Done at', get_current_time()]))

        logging.info(' '.join(['[%s]' % current_taks, type, 'zbjl_rc', '[ today_rc_count: %d ]' % today_rc_count, 'Done at', get_current_time()]))
        logging.info('-' * 100)