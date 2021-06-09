# coding=utf-8
# 首日启动,抓取过去累计4个月(121天)的直播url_zbjl/直播时间livestraming_time等部分关键数据(如5月31日抓全从1月31日到5月30日)
# 次日启动,每天抓取昨天直播记录所有详情数据(如6月1日抓取5月31日的)

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
logging.basicConfig(format='%(message)s',filename=today_log_dir + '/zbjl.log', level=logging.INFO)
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

def download_websocket_data(webcast_id, cookie, type, num_zb, url_zbjl):

    websocket_url = 'wss://xd.newrank.cn/xdnphb/nr/cloud/douyin/websocket'
    ws_data = {
        "type": "webcast",
        "data": {"room_id": webcast_id}
    }
    Retry_times = 20
    Severe_error_flag = False
    while 1:
        try:
            time.sleep(1)
            tipRank_data, commodity_data, detail_data = [None] * 3
            ws = websocket.WebSocket()
            ws.timeout = 10
            ws.connect(websocket_url, cookie=cookie, origin="https://xd.newrank.cn", host="xd.newrank.cn")
            ws.send(json.dumps(ws_data))

            while 1:
                try:
                    recv_data = json.loads(ws.recv())
                    if recv_data.get('type') == 'tipRank':
                        tipRank_data = recv_data.get('data')
                    if recv_data.get('type') == 'commodity':
                        commodity_data = recv_data.get('data')
                    if recv_data.get('type') == 'detail':
                        detail_data = recv_data.get('data')
                    #未带货直播的commodity_data为[],此时也需要跳出while
                    if commodity_data!=None and detail_data!=None:
                        break
                #except websocket.WebSocketTimeoutException:
                except:
                    if commodity_data and detail_data:
                        break
                    else:
                        raise Exception
        except:
            Retry_times -= 1
            logging.info('[*] Get zbjl websocket data failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, num_zb, url_zbjl,get_current_time()))
            if Retry_times == 0:
                Severe_error_flag = True
                break
            time.sleep(5)
        else:
            break

    if Severe_error_flag:
        return False

    with open('/root/xd_crawler/websocket_data/%s.detail'%webcast_id, 'w') as f:
        json.dump(detail_data,f)

    with open('/root/xd_crawler/websocket_data/%s.commodity' % webcast_id, 'w') as f:
        json.dump(commodity_data, f)

    return True


Entry_list = {
    ' Daily ': True,
    'History': True
}
websocket_use_count = 0
for current_taks in Entry_list:

    for type in input_type:
        today_zbjl_count = 0

        if current_taks == ' Daily ':
            update_date = lastday_date

        elif current_taks == 'History':

            all_history_data_query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.time_update.endswith('History')).order_by(list_%s_zbjl.livestraming_time).limit(10)" % (type, type, type)
            today_history_data_query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.time_update.endswith('History'),list_%s_zbjl.time_update.startswith('%s')).order_by(list_%s_zbjl.livestraming_time).limit(10)" % (type, type, type, today_date, type)

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

        for one_record in eval('list_' + type + '.select()'):

            uid = one_record.url_zb.split('/')[-1]
            webcastList_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/webcastList'

            post_data = {
                "create_time": update_date,
                "date_type": "",
                "end_time": update_date,
                "size": 100,
                "sort": "",
                "start": 1,
                "has_commerce_goods": "",
                "uid": uid
            }

            while 1:
                try:
                    rsp = requests.post(webcastList_url, headers=headers, data=json.dumps(post_data))
                    data_list = json.loads(rsp.text).get('data').get('list')
                except:
                    logging.info('[%s] Get zbjl webcastList_url data failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (current_taks, type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                    time.sleep(5)
                else:
                    break
            zbjl_count = 0
            for item in data_list:

                webcast_id = item.get('id')
                url_zbjl = 'https://xd.newrank.cn/d/broadcast/%s' % webcast_id

                repeat_detect_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.url_zbjl=='%s')" % (type, type, url_zbjl)
                if eval(repeat_detect_cmd):
                    zbjl_count += 1
                    today_zbjl_count += 1
                    logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl', one_record.num_zb, one_record.name_zb, webcast_id, item.get('create_time'), 'This is Repeated data. Continue next at', get_current_time()]))
                    continue

                Table_obj = eval('list_' + type + '_zbjl' + '.create()')
                Table_obj.num_zb = one_record.num_zb
                Table_obj.id_zb = one_record.id_zb
                Table_obj.name_zb = one_record.name_zb
                Table_obj.url_zbjl = url_zbjl
                Table_obj.livestraming_time = item.get('create_time')
                Table_obj.theme = item.get('title')

                if download_websocket_data(webcast_id, cookie, type, one_record.num_zb, url_zbjl):
                    websocket_use_count += 1
                else:
                    Table_obj.time_update = 'Severe error occurred at %s' % get_current_time()
                    Table_obj.save()
                    zbjl_count += 1
                    logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl', one_record.num_zb, one_record.name_zb, webcast_id, item.get('create_time'), 'Download websocket data failed at', get_current_time()]))
                    continue

                with open('/root/xd_crawler/websocket_data/%s.detail'%webcast_id, 'r') as f:
                    detail_data = json.load(f)

                Table_obj.duration = detail_data.get('duration')
                Table_obj.yinlangshouru = detail_data.get('stats_fan_ticket_money')
                Table_obj.benchangyinlang = detail_data.get('stats_fan_ticket')
                Table_obj.songli = detail_data.get('stats_gift_uv_count')
                Table_obj.zaixianfengzhi = detail_data.get('max_user_count')
                Table_obj.leijiguankan = detail_data.get('stats_total_user')
                Table_obj.zongdianzan = detail_data.get('like_count')
                Table_obj.danchangzhangfen = detail_data.get('add_fans_count')
                Table_obj.zhuanfenlv = detail_data.get('turn_rate')
                Table_obj.pingjunzaixian = detail_data.get('avg_user_count')
                Table_obj.pingjunzhiliu = detail_data.get('user_average_duration')

                Table_obj.yuguxiaoshoue = str(detail_data.get('total_sales_money'))
                Table_obj.yuguxiaoshouliang = detail_data.get('total_sales')
                Table_obj.shangjiashangpin = detail_data.get('promotion_count')

                Table_obj.zuigaodanjia = detail_data.get('max_price')
                Table_obj.zuigaoxiaoliang = detail_data.get('max_sales')
                Table_obj.zuigaoxiaoshoue = detail_data.get('max_sales_money')
                Table_obj.kedanjia = str(detail_data.get('customer_price'))
                Table_obj.renjungoumaijiazhi = detail_data.get('per_capita')
                Table_obj.xiaoshouzhuanhualv = detail_data.get('conversion_rate')

                if current_taks == ' Daily ':
                    Table_obj.time_update = get_current_time()
                elif current_taks == 'History':
                    Table_obj.time_update = get_current_time() + ' History'

                Table_obj.save()
                zbjl_count += 1
                today_zbjl_count +=1
                logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl', one_record.num_zb, one_record.name_zb, webcast_id, Table_obj.livestraming_time, 'Done at', get_current_time()]))

            logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl', one_record.num_zb, one_record.name_zb, '[ zbjl amount: %d ]'%zbjl_count, 'Done at', get_current_time()]))
            logging.info('-'*50)
        logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl', '[ today_zbjl_count: %d ]'%today_zbjl_count, 'Done at', get_current_time()]))
        logging.info('-' * 100)