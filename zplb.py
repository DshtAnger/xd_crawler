# coding=utf-8
# 日更抓取昨天的作品url数据(如6月10日抓取6月9日),历史抓取4个月(121天)前的作品url相关数据(如6月10日抓取2月9日)

import requests
import json
import datetime
import time
import os,sys,re
import logging
from DB import *

def get_current_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

today_date = datetime.datetime.now().strftime("%Y-%m-%d")
lastday_date = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")

today_log_dir = '/root/xd_crawler/log/%s' % today_date
if not os.path.exists(today_log_dir):
    os.mkdir(today_log_dir)
logging.basicConfig(format='%(message)s',filename=today_log_dir + '/zplb.log', level=logging.INFO)
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
# 设置抓历史的停止条件,带History标记的最大time_release日期+1 == 不带History标记的最小time_release时
for current_taks in Entry_list:

    for type in input_type:
        today_zplb_count = 0

        if current_taks == ' Daily ':
            update_date = lastday_date

        elif current_taks == 'History':

            all_history_data_query_cmd = "list_%s_zplb.select().where(list_%s_zplb.time_update.endswith('History')).order_by(-list_%s_zplb.time_release).limit(10)" % (type, type, type)
            today_history_data_query_cmd = "list_%s_zplb.select().where(list_%s_zplb.time_update.endswith('History'),list_%s_zplb.time_update.startswith('%s')).order_by(-list_%s_zplb.time_release).limit(10)" % (type, type, type, today_date, type)

            all_query_result = eval(all_history_data_query_cmd)
            today_query_result = eval(today_history_data_query_cmd)

            if bool(all_query_result):
                # 可能是非首日,也可能是首日、但程序中断存在部分首日历史数据
                if bool(today_query_result):
                    # 首日/非首日的第二次运行,即程序异常导致数据中断情景
                    latest_history_date = today_query_result[0].time_release.split(' ')[0]
                    update_date = latest_history_date
                else:
                    # 非首日,日更运行场景
                    latest_history_date = all_query_result[0].time_release.split(' ')[0]
                    update_date = (datetime.datetime.strptime(latest_history_date, "%Y-%m-%d") + datetime.timedelta(days=+1)).strftime("%Y-%m-%d")
            else:
                # 首日第一次运行,数据库中没有任何带标记的历史数据，即也不存在任何今日历史数据
                update_date = (datetime.datetime.strptime(today_date, "%Y-%m-%d") + datetime.timedelta(days=-121)).strftime("%Y-%m-%d")

            earliest_no_history_query = "list_%s_zplb.select().where(~list_%s_zplb.time_update.endswith('History')).order_by(list_%s_zplb.time_release).limit(10)" % (type, type, type)
            earliest_no_history_date = eval(earliest_no_history_query)[0].time_release.split(' ')[0]

            if update_date == earliest_no_history_date:
                logging.info(' '.join(['[%s]' % current_taks, type, 'zb_zplb', '[ Does not crawl history data any more ]', 'Done at', get_current_time()]))
                logging.info('-' * 100)
                continue

        for one_record in eval('list_%s.select().where(list_%s.time_update=="%s")'%(type, type, today_date)):

            uid = one_record.url_zb.split('/')[-1]

            aweme_id_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/detail/aweme'

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
                "create_time_start": update_date,
                "create_time_end": update_date
            }
            zplb_count = 0

            Retry_times = 10
            continue_next_flag = False
            while 1:
                try:
                    rsp = requests.post(aweme_id_url, headers=headers, data=json.dumps(post_data))
                    data_ori = json.loads(rsp.text)
                    data = data_ori.get('data')
                except:
                    Retry_times -= 1
                    logging.info('[*] Get zb_zplb aweme_id_url failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, get_current_time()))
                    if Retry_times == 0:
                        continue_next_flag = True
                        break
                    time.sleep(5)
                else:
                    break
            if continue_next_flag:
                logging.info(' '.join(['[%s]' % current_taks, type, 'zb_zplb', one_record.num_zb, one_record.name_zb, 'aweme_id_url Error at', get_current_time()]))
                continue

            if data_ori.get('code') == 4014 and data == None:
                # '{\n\t"msg":"<div class=\\"xd_noauth_wrap\\"><div class=\\"xd_noauth_title\\">今日访问次数已达上限</div><div class=\\"xd_noauth_subtitle\\">您当前为超级全家桶，该页面访问次数为<span class=\\"xd_noauth_count\\">8000</span>次/天</div></div>",\n\t"data":null,\n\t"code":4014\n}'
                error_msg = '|'.join(re.search(r'<div class="xd_noauth_wrap"><div class="xd_noauth_title">(.*)</div><div class="xd_noauth_subtitle">(.*)<span class="xd_noauth_count">(.*)</span>(.*)</div></div>',data_ori.get('msg')).groups())
                logging.info(' '.join(['[*] %s' % error_msg, one_record.num_zb, one_record.name_zb, 'at', get_current_time()]))
                os._exit(1)
            else:
                 data_list = data.get('list')

            for aweme_obj in data_list:

                aweme_id = aweme_obj.get('aweme_id')
                time_release = aweme_obj.get('create_time')

                url_works = "https://xd.newrank.cn/material/detail/comment/%s" % aweme_id

                repeat_detect_cmd = "list_%s_zplb.select().where(list_%s_zplb.url_works=='%s',list_%s_zplb.time_update.startswith('%s'))" % (type, type, url_works, type, today_date)
                if eval(repeat_detect_cmd):
                    logging.info(' '.join(['[%s]' % current_taks, type, 'zb_zplb', one_record.num_zb, one_record.name_zb, aweme_id, time_release, 'This is Repeated data. Continue next at', get_current_time()]))
                    continue

                Table_obj = eval('list_' + type + '_zplb' + '.create()')

                Table_obj.num_zb = one_record.num_zb
                Table_obj.id_zb = one_record.id_zb
                Table_obj.name_zb = one_record.name_zb
                Table_obj.url_zb = one_record.url_zb
                Table_obj.url_works = url_works

                Table_obj.content = aweme_obj.get('aweme_desc')
                Table_obj.duration = aweme_obj.get('duration')
                Table_obj.time_release = time_release
                Table_obj.music = aweme_obj.get('music_info').get('title') if aweme_obj.get('music_info') else '--'

                if current_taks == ' Daily ':
                    Table_obj.time_update = get_current_time()
                elif current_taks == 'History':
                    Table_obj.time_update = get_current_time() + ' History'

                Table_obj.save()
                zplb_count +=1
                today_zplb_count += 1
                logging.info(' '.join(['[%s]' % current_taks, type, 'zb_zplb', one_record.num_zb, one_record.name_zb, aweme_id, time_release, 'Done at', get_current_time()]))

            logging.info(' '.join(['[%s]' % current_taks, type, 'zb_zplb', one_record.num_zb, one_record.name_zb, '[ zplb_count: %d ]' % zplb_count, 'Done at', get_current_time()]))
            logging.info('-' * 50)

        logging.info(' '.join(['[%s]' % current_taks, type, 'zb_zplb', '[ today_zplb_count: %d ]' % today_zplb_count, 'Done at', get_current_time()]))
        logging.info('-' * 100)