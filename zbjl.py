# coding=utf-8
# 从6月1日期开始每日更新，每天抓取昨天的数据（如6月1日抓取5月31日的）

import requests
import websocket
import json
import datetime
import time
from DB import *


today_date = datetime.datetime.now().strftime("%Y-%m-%d")
lastday_date = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
first_crawl_date = (datetime.datetime.now()+datetime.timedelta(days=-121)).strftime("%Y-%m-%d")

type_list = {
    'ms':'美食','ss':'时尚','kj':'科技',
    'yl':'娱乐','gx':'搞笑','cy':'才艺',
    'qy':'企业',
}

with open('./cookie','r') as f:
    cookie = f.read().strip()
with open('./token','r') as f:
    token = f.read().strip()
with open('./type','r') as f:
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
        webcastList_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/webcastList'
        post_data = {
            "create_time": lastday_date,
            "date_type": "",
            "end_time": lastday_date,
            "size": 100,
            "sort": "",
            "start": 1,
            "has_commerce_goods": "",
            "uid": uid
        }
        while 1:
            try:
                rsp = requests.post(webcastList_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data')
                count = data.get('count')
                end_page = int(count / 100) + 1 if count % 100 != 0 else int(count / 100)
            except:
                print(
                    '[*] Get zbjl webcastList_url count failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb,time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                time.sleep(5)
            else:
                break

        for page in range(1,end_page+1):

            while 1:
                try:
                    post_data.update({'start': page})
                    rsp = requests.post(webcastList_url, headers=headers, data=json.dumps(post_data))
                    data_list = json.loads(rsp.text).get('data').get('list')
                except:
                    print(
                        '[*] Get zbjl webcastList_url data failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl,time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                    time.sleep(5)
                else:
                    break

            for item in data_list:

                webcast_id = item.get('id')
                url_zbjl = 'https://xd.newrank.cn/d/broadcast/%s' % webcast_id

                query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.url_zbjl=='%s',list_%s_zbjl.time_update.startswith('%s'))" % (
                type, type, url_zbjl, type, today_date)
                if eval(query_cmd):
                    print('[+]', type, 'zbjl', one_record.num_zb, one_record.name_zb, webcast_id, 'Done at',
                          time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                    continue

                Table_obj = eval('list_' + type + '_zbjl' + '.create()')
                Table_obj.num_zb = one_record.num_zb
                Table_obj.id_zb = one_record.id_zb
                Table_obj.name_zb = one_record.name_zb
                Table_obj.url_zbjl = url_zbjl

                Table_obj.theme = item.get('title')

                websocket_url = 'wss://xd.newrank.cn/xdnphb/nr/cloud/douyin/websocket'
                ws_data = {
                    "type": "webcast",
                    "data": {"room_id": webcast_id}
                }
                while 1:
                    try:
                        #time.sleep(1)
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
                                #data字段可能为[]
                                if commodity_data!=None and detail_data!=None:
                                    break
                            #except websocket.WebSocketTimeoutException:
                            except:
                                if commodity_data and detail_data:
                                    break
                                else:
                                    raise Exception
                    except:
                        print('[*] Get zbjl websocket data faile. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, Table_obj.url_zbjl,time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                        time.sleep(5)
                    else:
                        break


                Table_obj.livestraming_time = detail_data.get('create_time')
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
                Table_obj.time_update = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                Table_obj.save()

                print('[+]', type, 'zbjl', one_record.num_zb, one_record.name_zb, webcast_id, detail_data.get('create_time'), 'Done at',time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))