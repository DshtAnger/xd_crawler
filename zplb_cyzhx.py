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

update_date = (datetime.datetime.now()+datetime.timedelta(days=-121)).strftime("%Y-%m-%d")
today_date = datetime.datetime.now().strftime("%Y-%m-%d")

today_log_dir = '/root/xd_crawler/log/%s' % today_date
if not os.path.exists(today_log_dir):
    os.mkdir(today_log_dir)
logging.basicConfig(format='%(message)s',filename=today_log_dir + '/zplb_cyzhx.log', level=logging.INFO)
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
    today_cyzhx_count  = 0

    # # for ms server's data 2 days miss
    # if type in ['ms','ly','kj']:
    #     update_date = (datetime.datetime.now() + datetime.timedelta(days=-121-2)).strftime("%Y-%m-%d")
    # # for gx server's data 1 days miss
    # if type in ['gx','ss','jk']:
    #     update_date = (datetime.datetime.now() + datetime.timedelta(days=-121-1)).strftime("%Y-%m-%d")

    query_cmd = "list_%s_zplb_cyzhx.select().order_by(-list_%s_zplb_cyzhx.time_update).limit(10)" % (type, type)
    query_result = eval(query_cmd)
    url_works = list(query_result)[0].url_works
    query_cmd = "list_%s_zplb.select().where(list_%s_zplb.url_works=='%s')" % (type, type, url_works)
    query_result = eval(query_cmd)
    time_release = list(query_result)[0].time_release.split(' ')[0]

    for i in range(1,100):
        update_date = (datetime.datetime.strptime(time_release, "%Y-%m-%d") + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        query_cmd = "list_%s_zplb.select().where(list_%s_zplb.time_release.startswith('%s'))" % (type,type,update_date)
        query_result = eval(query_cmd)
        if bool(query_result):
            break

    for one_record in query_result:

        aweme_id = one_record.url_works.split('/')[-1]
        aweme_userinfo_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/aweme/awemeDetail/getAwemeUserInfo'
        post_data = {
            "aweme_id": aweme_id
        }
        Retry_times = 3
        continue_next_flag = False
        while 1:
            try:
                rsp = requests.post(aweme_userinfo_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data')
            except:
                Retry_times -= 1
                logging.info('[*] Get zb_zplb_cyzhx failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, get_current_time()))
                if Retry_times == 0:
                    continue_next_flag = True
                    break
                time.sleep(5)
            else:
                break
        if continue_next_flag:
            logging.info(' '.join(['[+]', type, 'zb_zplb_cyzhx', one_record.num_zb, one_record.name_zb, aweme_id, one_record.time_release, "aweme_userinfo_url Error at", get_current_time()]))
            continue

        Table_obj = eval('list_' + type + '_zplb_cyzhx' + '.create()')

        Table_obj.num_zb = one_record.num_zb
        Table_obj.id_zb = one_record.id_zb
        Table_obj.name_zb = one_record.name_zb
        Table_obj.url_zb = one_record.url_zb
        Table_obj.url_works = one_record.url_works

        if not data:
            #Table_obj.female,Table_obj.male,Table_obj.eighteen, Table_obj.eighteentotwentythree, Table_obj.twentyfourtothirty, Table_obj.thirtyonetofourty, Table_obj.fourtyonetofifty, Table_obj.fifty = ['--'] * 8
            #Table_obj.time_update = get_current_time()
            #Table_obj.save()
            logging.info(' '.join(['[+]', type, 'zb_zplb_cyzhx', one_record.num_zb, one_record.name_zb, aweme_id, one_record.time_release, "No data at", get_current_time()]))
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
        today_cyzhx_count += 1

        logging.info(' '.join(['[+]', type, 'zb_zplb_cyzhx', one_record.num_zb, one_record.name_zb, aweme_id, one_record.time_release, "Done at", get_current_time()]))

    logging.info(' '.join(['[+]', type, 'zb_zplb_cyzhx', '[ today_cyzhx_count :%d ]'%today_cyzhx_count, "Done at", get_current_time()]))
    logging.info('-' * 100)
