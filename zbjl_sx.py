# coding=utf-8
# 首日启动,什么都不做,轮空
# 次日启动,每天抓取昨天的相应数据(如6月1日抓取5月31日的)

import requests
import websocket
import json
import datetime
import time
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

for type in input_type:

    query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.livestraming_time.startswith('%s'))" % (type,type,lastday_date)

    for one_record in eval(query_cmd):

        query_cmd = "list_%s_zbjl_sx.select().where(list_%s_zbjl_sx.url_zbjl=='%s',list_%s_zbjl_sx.time_update.startswith('%s'))" % (
            type, type, one_record.url_zbjl, type, today_date)
        if eval(query_cmd):
            print('[+]', type, 'zbjl_sx', one_record.num_zb, one_record.name_zb, one_record.url_zbjl.split('/')[-1],
                  one_record.livestraming_time, 'Done at', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            continue

        Table_obj = eval('list_' + type + '_zbjl_sx' + '.create()')
        Table_obj.num_zb = one_record.num_zb
        Table_obj.id_zb = one_record.id_zb
        Table_obj.name_zb = one_record.name_zb
        Table_obj.url_zbjl = one_record.url_zbjl
        Table_obj.livestraming_time = one_record.livestraming_time

        webcast_id = one_record.url_zbjl.split('/')[-1]
        websocket_url = 'wss://xd.newrank.cn/xdnphb/nr/cloud/douyin/websocket'
        ws_data = {
            "type": "webcast",
            "data": {"room_id": webcast_id}
        }
        while 1:
            try:
                # time.sleep(1)
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
                        # data字段可能为[]
                        if commodity_data != None and detail_data != None:
                            break
                    # except websocket.WebSocketTimeoutException:
                    except:
                        if commodity_data and detail_data:
                            break
                        else:
                            raise Exception

            except:
                print('[*] Get zbjl_sx websocket data failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                time.sleep(5)
            else:
                break

        Table_obj.shipintuijian = detail_data.get('stats_user_count_composition').get('video_detail') if detail_data.get('stats_user_count_composition') else '--'
        Table_obj.guanzhu = detail_data.get('stats_user_count_composition').get('my_follow') if detail_data.get('stats_user_count_composition') else '--'
        Table_obj.zhiboguangchang = detail_data.get('stats_user_count_composition').get('other') if detail_data.get('stats_user_count_composition') else '--'

        male, female = ['--'] * 2
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
                print(
                    '[*] Get zbjl_sx city_url failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl,time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
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

        Table_obj.time_update = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        Table_obj.save()

        print('[+]', type, 'zbjl_sx', one_record.num_zb, one_record.name_zb, webcast_id, Table_obj.livestraming_time, 'Done at',time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))