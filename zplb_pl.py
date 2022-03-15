# coding=utf-8
# 每日更新，每天抓取是4个月前记录的作品url的数据（如6月1日抓取的是1月31日的）即向前推121天

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
update_date = (datetime.datetime.now()+datetime.timedelta(days=-121)).strftime("%Y-%m-%d")

today_log_dir = '/root/xd_crawler/log/%s' % today_date
if not os.path.exists(today_log_dir):
    os.mkdir(today_log_dir)
logging.basicConfig(format='%(message)s',filename=today_log_dir + '/zplb_pl.log', level=logging.INFO)
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
    today_pl_count = 0

    # # for ms server's data 2 days miss
    # if type in ['ms','ly','kj']:
    #     update_date = (datetime.datetime.now() + datetime.timedelta(days=-121-2)).strftime("%Y-%m-%d")
    # # for gx server's data 1 days miss
    # if type in ['gx','ss','jk']:
    #     update_date = (datetime.datetime.now() + datetime.timedelta(days=-121-1)).strftime("%Y-%m-%d")

    # query_cmd = "list_%s_zplb_pl.select().order_by(-list_%s_zplb_pl.time_update).limit(10)" % (type, type)
    # query_result = eval(query_cmd)
    # url_works = list(query_result)[0].url_works
    # query_cmd = "list_%s_zplb.select().where(list_%s_zplb.url_works=='%s')" % (type, type, url_works)
    # query_result = eval(query_cmd)
    # time_release = list(query_result)[0].time_release.split(' ')[0]
    #
    # for i in range(1,100):
    #     update_date = (datetime.datetime.strptime(time_release, "%Y-%m-%d") + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
    #     query_cmd = "list_%s_zplb.select().where(list_%s_zplb.time_release.startswith('%s'))" % (type,type,update_date)
    #     query_result = eval(query_cmd)
    #     if bool(query_result):
    #         break

    query_cmd = "list_%s_zplb.select().where(list_%s_zplb.time_release.startswith('%s'))" % (type, type, update_date)
    query_result = eval(query_cmd)

    for one_record in query_result:
        pl_count = 0

        aweme_id = one_record.url_works.split('/')[-1]
        aweme_comment_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/aweme/awemeDetail/listAwemeComment'
        post_data = {
            "aweme_id": aweme_id,
            "size": 100,
            "start": 1
        }
        Retry_times = 3
        continue_next_flag = False
        while 1:
            try:
                rsp = requests.post(aweme_comment_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data')
                count = data.get('count')
                end_page = int(count / 100) + 1 if count % 100 != 0 else int(count / 100)
            except:
                Retry_times -= 1
                logging.info('[*] Get zb_zplb_pl aweme_comment_url count failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, get_current_time()))
                if Retry_times == 0:
                    continue_next_flag = True
                    break
                time.sleep(5)
            else:
                break
        if continue_next_flag:
            logging.info(' '.join(['[+]', type, 'zb_zplb_pl', one_record.num_zb, one_record.name_zb, one_record.url_works, one_record.time_release, 'aweme_comment_url count Error at', get_current_time()]))
            continue

        for page in range(1,end_page+1):

            Retry_times = 3
            continue_next_flag = False
            while 1:
                try:
                    post_data.update({'start': page})
                    rsp = requests.post(aweme_comment_url, headers=headers, data=json.dumps(post_data))
                    data_list = json.loads(rsp.text).get('data').get('list')
                except:
                    Retry_times -= 1
                    logging.info('[*] Get zb_zplb_pl aweme_id_url data failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, get_current_time()))
                    if Retry_times == 0:
                        continue_next_flag = True
                        break
                    time.sleep(5)
                else:
                    break
            if continue_next_flag:
                logging.info(' '.join(['[+]', type, 'zb_zplb_pl', one_record.num_zb, one_record.name_zb, aweme_id, one_record.time_release, 'aweme_comment_url data Error at', get_current_time()]))
                break

            for item in data_list:

                Table_obj = eval('list_' + type + '_zplb_pl' + '.create()')

                Table_obj.num_zb = one_record.num_zb
                Table_obj.id_zb = one_record.id_zb
                Table_obj.name_zb = one_record.name_zb
                Table_obj.url_zb = one_record.url_zb
                Table_obj.url_works = one_record.url_works

                Table_obj.reviewer = item.get('user_imfor')[0].get('nickname') if item.get('user_imfor') else '--'
                Table_obj.xin = str(item.get('digg_count'))
                Table_obj.comment = item.get('text')
                Table_obj.time_comment = item.get('create_time')
                Table_obj.time_update = get_current_time()

                Table_obj.save()
                pl_count += 1
                today_pl_count += 1
        else:
            logging.info(' '.join(['[+]', type, 'zb_zplb_pl', one_record.num_zb, one_record.name_zb, aweme_id, one_record.time_release, '[ pl_count: %d ]' % pl_count ,"Done at", get_current_time()]))

    logging.info(' '.join(['[+]', type, 'zb_zplb_pl', '[ today_pl_count: %d ]' % today_pl_count, "Done at", get_current_time()]))
    logging.info('-' * 100)
