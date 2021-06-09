# coding=utf-8
# 首日启动,什么都不做,轮空
# 次日启动,每天抓取昨天的相应数据(如6月1日抓取5月31日的)

import requests
import websocket
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
logging.basicConfig(format='%(message)s',filename=today_log_dir + '/zbjl_pl.log', level=logging.INFO)
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.info("--------------------Uncaught Exception--------------------",exc_info=(exc_type, exc_value, exc_traceback))
sys.excepthook = handle_exception

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


Entry_list = {
    ' Daily ': True,
    'History': True
}
for current_taks in Entry_list:

    for type in input_type:
        today_pl_count = 0

        if current_taks == ' Daily ':
            update_date = lastday_date

        elif current_taks == 'History':

            all_history_data_query_cmd = "list_%s_zbjl_pl.select().where(list_%s_zbjl_pl.time_update.endswith('History')).order_by(list_%s_zbjl_pl.livestraming_time).limit(10)" % (type, type, type)
            today_history_data_query_cmd = "list_%s_zbjl_pl.select().where(list_%s_zbjl_pl.time_update.endswith('History'),list_%s_zbjl_pl.time_update.startswith('%s')).order_by(list_%s_zbjl_pl.livestraming_time).limit(10)" % (type, type, type, today_date, type)

            all_query_result = eval(all_history_data_query_cmd)
            today_query_result = eval(today_history_data_query_cmd)

            if bool(all_query_result):
                # 可能是非首日,也可能是首日、但程序中断存在部分首日历史数据
                if bool(today_query_result):
                    # 首日/非首日的第二次运行,即程序异常导致数据中断情景
                    latest_history_date = today_query_result[0].livestraming_time.split(' ')[0]
                    update_date = latest_history_date
                else:
                    # 非首日,日更运行场景
                    latest_history_date = all_query_result[0].livestraming_time.split(' ')[0]
                    update_date = (datetime.datetime.strptime(latest_history_date, "%Y-%m-%d") + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
            else:
                # 首日第一次运行,数据库中没有任何带标记的历史数据，即也不存在任何今日历史数据
                latest_history_date = lastday_date
                update_date = (datetime.datetime.strptime(latest_history_date, "%Y-%m-%d") + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")

        query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.livestraming_time.startswith('%s'))" % (type, type, update_date)

        for one_record in eval(query_cmd):

            webcast_id = one_record.url_zbjl.split('/')[-1]
            comment_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/webcastMessageList'
            post_data = {
                "room_id": webcast_id,
                "size": 100,
                "start": 1,
            }
            while 1:
                try:
                    rsp = requests.post(comment_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data')
                    count = data.get('count') if data else -1
                    end_page = int(count / 100) + 1 if count % 100 != 0 else int(count / 100)
                except:
                    logging.info('[*] Get zbjl_pl comment_url count failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                    time.sleep(5)
                else:
                    break

            if count == -1:
                Table_obj = eval('list_' + type + '_zbjl_pl' + '.create()')
                Table_obj.num_zb = one_record.num_zb
                Table_obj.id_zb = one_record.id_zb
                Table_obj.name_zb = one_record.name_zb
                Table_obj.url_zbjl = one_record.url_zbjl
                Table_obj.livestraming_time = one_record.livestraming_time
                Table_obj.time_update = 'This zbjl No_pl_data. Maybe the limit is exhausted' % get_current_time()
                logging.info('[%s]'%current_taks, type, 'zbjl_pl', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, 'This zbjl No_pl_data. Maybe the limit is exhausted. Continue next at ', get_current_time())
                continue

            pl_count = 0
            for page in range(1, end_page + 1):

                while 1:
                    try:
                        post_data.update({'start': page})
                        rsp = requests.post(comment_url, headers=headers, data=json.dumps(post_data))
                        data = json.loads(rsp.text).get('data')
                        data_list = data.get('list') if data else []
                    except:
                        logging.info('[*] Get zbjl_pl comment_url data failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                        time.sleep(5)
                    else:
                        break

                for item in data_list:

                    Table_obj = eval('list_' + type + '_zbjl_pl' + '.create()')

                    Table_obj.num_zb = one_record.num_zb
                    Table_obj.id_zb = one_record.id_zb
                    Table_obj.name_zb = one_record.name_zb
                    Table_obj.url_zbjl = one_record.url_zbjl
                    Table_obj.livestraming_time = one_record.livestraming_time

                    Table_obj.review_time = item.get('create_time')
                    Table_obj.author = item.get('user').get('nickname')
                    Table_obj.content = item.get('content')

                    if current_taks == ' Daily ':
                        Table_obj.time_update = get_current_time()
                    elif current_taks == 'History':
                        Table_obj.time_update = get_current_time() + ' History'

                    Table_obj.save()
                    pl_count += 1
                    today_pl_count += 1

            logging.info('[%s]'%current_taks, type, 'zbjl_pl', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, '[ zbjl_pl amount: %d ]'%pl_count, 'Done at', get_current_time())
        logging.info('[%s]'%current_taks, type, 'zbjl_pl', '[ today_pl_count: %d ]'%today_pl_count, 'Done at', get_current_time())
        logging.info('-'*100)