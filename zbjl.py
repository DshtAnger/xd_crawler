# coding=utf-8
# 首日启动,抓取过去累计4个月(121天)的直播url_zbjl/直播时间livestraming_time等部分关键数据(如5月31日抓全从1月31日到5月30日)
# 次日启动,每天抓取昨天直播记录所有详情数据(如6月1日抓取5月31日的)

import requests
import json
import websocket
import datetime,time
from DB import *

def get_current_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

today_date = datetime.datetime.now().strftime("%Y-%m-%d")
lastday_date = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
first_crawl_date = (datetime.datetime.now()+datetime.timedelta(days=-121)).strftime("%Y-%m-%d")
FIRST_RUN_DATE = '2021-06-02'

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
    while 1:
        try:
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
            print('[*] Get zbjl websocket data failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, num_zb, url_zbjl,get_current_time()))
            time.sleep(5)
        else:
            break

    with open('/root/xd_crawler/websocket_data/%s.detail'%webcast_id, 'w') as f:
        json.dump(detail_data,f)

    with open('/root/xd_crawler/websocket_data/%s.commodity' % webcast_id, 'w') as f:
        json.dump(commodity_data, f)

    return True

websocket_use_count = 0
for type in input_type:

    for one_record in eval('list_' + type + '.select()'):

        uid = one_record.url_zb.split('/')[-1]
        webcastList_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/webcastList'

        if today_date == FIRST_RUN_DATE:
            update_date = first_crawl_date
        else:
            update_date = lastday_date

        post_data = {
            "create_time": update_date,
            "date_type": "",
            "end_time": lastday_date,
            "size": 100,
            "sort": "",
            "start": 1,
            "has_commerce_goods": "",
            "uid": uid
        }
        zbjl_count = 0
        while 1:
            try:
                rsp = requests.post(webcastList_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data')
                count = data.get('count')
                end_page = int(count / 100) + 1 if count % 100 != 0 else int(count / 100)
            except:
                print('[*] Get zbjl webcastList_url count failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb,get_current_time()))
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
                    print('[*] Get zbjl webcastList_url data failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl,get_current_time()))
                    time.sleep(5)
                else:
                    break

            for item in data_list:

                webcast_id = item.get('id')
                url_zbjl = 'https://xd.newrank.cn/d/broadcast/%s' % webcast_id

                query_cmd1 = "list_%s_zbjl.select().where(list_%s_zbjl.url_zbjl=='%s',list_%s_zbjl.time_update.startswith('%s'))" % (type, type, url_zbjl, type, today_date)
                #query_cmd2 = "list_%s_zbjl.select().where(list_%s_zbjl.url_zbjl=='%s',list_%s_zbjl.time_update.startswith('%s'))" % (type, type, url_zbjl, type, 'First_Established')
                eval1 = eval(query_cmd1)
                #eval2 = eval(query_cmd2)

                if eval1:# or eval2:
                    print('[+]', type, 'zbjl', one_record.num_zb, one_record.name_zb, webcast_id, item.get('create_time'), 'Done at', get_current_time())
                    zbjl_count += 1
                    continue

                Table_obj = eval('list_' + type + '_zbjl' + '.create()')
                Table_obj.num_zb = one_record.num_zb
                Table_obj.id_zb = one_record.id_zb
                Table_obj.name_zb = one_record.name_zb
                Table_obj.url_zbjl = url_zbjl
                Table_obj.livestraming_time = item.get('create_time')
                Table_obj.theme = item.get('title')

                if today_date == FIRST_RUN_DATE:
                    Table_obj.time_update = 'First_Established at %s' % get_current_time()
                    Table_obj.save()
                    zbjl_count += 1

                else:
                    download_websocket_data(webcast_id, cookie, type, one_record.num_zb, one_record.num_zb)
                    websocket_use_count += 1

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
                    Table_obj.time_update = get_current_time()

                    Table_obj.save()
                    zbjl_count += 1

                print('[+]', type, 'zbjl', one_record.num_zb, one_record.name_zb, webcast_id, Table_obj.livestraming_time, 'Done at', get_current_time())

        print('[+]', type, 'zbjl', one_record.num_zb, one_record.name_zb, '[ zbjl amount: %d ]'%zbjl_count, 'Done at', get_current_time())