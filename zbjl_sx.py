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
logging.basicConfig(format='%(message)s',filename=today_log_dir + '/zbjl_sx.log', level=logging.INFO)
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
        today_zbjl_sx_count = 0

        if current_taks == ' Daily ':
            update_date = lastday_date

        elif current_taks == 'History':

            all_history_data_query_cmd = "list_%s_zbjl_sx.select().where(list_%s_zbjl_sx.time_update.endswith('History')).order_by(list_%s_zbjl_sx.livestraming_time).limit(10)" % (type, type, type)
            today_history_data_query_cmd = "list_%s_zbjl_sx.select().where(list_%s_zbjl_sx.time_update.endswith('History'),list_%s_zbjl_sx.time_update.startswith('%s')).order_by(list_%s_zbjl_sx.livestraming_time).limit(10)" % (type, type, type, today_date, type)

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

        query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.livestraming_time.startswith('%s')).order_by(list_%s_zbjl.time_update)" % (type, type, update_date, type)

        for one_record in eval(query_cmd):

            webcast_id = one_record.url_zbjl.split('/')[-1]

            repeat_detect_cmd = "list_%s_zbjl_sx.select().where(list_%s_zbjl_sx.url_zbjl=='%s',list_%s_zbjl_sx.time_update.startswith('%s'))" % (type, type, one_record.url_zbjl, type, today_date)
            if eval(repeat_detect_cmd):
                logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl_sx', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, 'This is Repeated data. Continue next at', get_current_time()]))
                continue

            Table_obj = eval('list_' + type + '_zbjl_sx' + '.create()')
            Table_obj.num_zb = one_record.num_zb
            Table_obj.id_zb = one_record.id_zb
            Table_obj.name_zb = one_record.name_zb
            Table_obj.url_zbjl = one_record.url_zbjl
            Table_obj.livestraming_time = one_record.livestraming_time

            if not os.path.exists('/root/xd_crawler/websocket_data/%s.detail' % webcast_id):
                Table_obj.time_update = 'Severe error occurred at %s' % get_current_time()
                Table_obj.save()
                logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl_sx', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, 'Find local websocket file failed  at', get_current_time()]))
                continue

            with open('/root/xd_crawler/websocket_data/%s.detail' % webcast_id, 'r') as f:
                detail_data = json.load(f)

            Table_obj.shipintuijian = detail_data.get('stats_user_count_composition').get('video_detail') if detail_data.get('stats_user_count_composition') else '--'
            Table_obj.guanzhu = detail_data.get('stats_user_count_composition').get('my_follow') if detail_data.get('stats_user_count_composition') else '--'
            Table_obj.zhiboguangchang = detail_data.get('stats_user_count_composition').get('other') if detail_data.get('stats_user_count_composition') else '--'

            Table_obj.male, Table_obj.female = ['--'] * 2
            for gender in detail_data.get('watch_user_gender'):
                if gender.get('key') == "1":
                    Table_obj.male = gender.get('rate')
                if gender.get('key') == "2":
                    Table_obj.female = gender.get('rate')


            city_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/webDetail/city'
            post_data = {
                "room_id": webcast_id
            }
            while 1:
                try:
                    rsp = requests.post(city_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data')
                except:
                    logging.info('[*] Get zbjl_sx city_url failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl,get_current_time()))
                    time.sleep(5)
                else:
                    break

            Table_obj.heilongjiang = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '黑龙江', data)]) if data!=None else '--'
            Table_obj.jilin = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '吉林', data)]) if data!=None else '--'
            Table_obj.liaoning = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '辽宁', data)]) if data!=None else '--'
            Table_obj.neimeng = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '内蒙古', data)]) if data!=None else '--'
            Table_obj.hebei = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '河北', data)]) if data!=None else '--'
            Table_obj.beijing = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '北京', data)]) if data!=None else '--'
            Table_obj.tianjin = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '天津', data)]) if data!=None else '--'
            Table_obj.shandong = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '山东', data)]) if data!=None else '--'
            Table_obj.shanxi = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '山西', data)]) if data!=None else '--'
            Table_obj.henan = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '河南', data)]) if data!=None else '--'
            Table_obj.anhui = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '安徽', data)]) if data!=None else '--'
            Table_obj.jiangsu = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '江苏', data)]) if data!=None else '--'
            Table_obj.shanghai = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '上海', data)]) if data!=None else '--'
            Table_obj.zhejing = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '浙江', data)]) if data!=None else '--'
            Table_obj.jiangxi = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '江西', data)]) if data!=None else '--'
            Table_obj.fujian = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '福建', data)]) if data!=None else '--'
            Table_obj.taiwan = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '台湾', data)]) if data!=None else '--'
            Table_obj.guangdong = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '广东', data)]) if data!=None else '--'
            Table_obj.guangxi = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '广西', data)]) if data!=None else '--'
            Table_obj.hainan = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '海南', data)]) if data!=None else '--'
            Table_obj.yunnan = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '云南', data)]) if data!=None else '--'
            Table_obj.guizhou = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '贵州', data)]) if data!=None else '--'
            Table_obj.hunan = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '湖南', data)]) if data!=None else '--'
            Table_obj.chongqing = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '重庆', data)]) if data!=None else '--'
            Table_obj.hubei = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '湖北', data)]) if data!=None else '--'
            Table_obj.shaanxi = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '陕西', data)]) if data!=None else '--'
            Table_obj.ningxia = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '宁夏', data)]) if data!=None else '--'
            Table_obj.gansu = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '甘肃', data)]) if data!=None else '--'
            Table_obj.sichuan = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '四川', data)]) if data!=None else '--'
            Table_obj.qinghai = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '青海', data)]) if data!=None else '--'
            Table_obj.xinjiang = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '新疆', data)]) if data!=None else '--'
            Table_obj.xizhang = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '西藏', data)]) if data!=None else '--'
            Table_obj.xianggang = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '香港', data)]) if data!=None else '--'
            Table_obj.aomen = ''.join([i.get('rate') for i in filter(lambda x: x.get('key') == '澳门', data)]) if data!=None else '--'

            if current_taks == ' Daily ':
                Table_obj.time_update = get_current_time()
            elif current_taks == 'History':
                Table_obj.time_update = get_current_time() + ' History'

            Table_obj.save()
            today_zbjl_sx_count += 1

            logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl_sx', one_record.num_zb, one_record.name_zb, webcast_id, Table_obj.livestraming_time, 'Done at', get_current_time()]))
        logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl', '[ today_zbjl_sx_count: %d ]'%today_zbjl_sx_count, 'Done at', get_current_time()]))
        logging.info('-'*100)