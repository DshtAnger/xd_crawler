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



def zbjl_sx(query_cmd, today_update_flag):

    for one_record in eval(query_cmd):

        webcast_id = one_record.url_zbjl.split('/')[-1]

        query_cmd = "list_%s_zbjl_sx.select().where(list_%s_zbjl_sx.url_zbjl=='%s',list_%s_zbjl_sx.time_update.startswith('%s'))" % (type, type, one_record.url_zbjl, type, today_date)
        if eval(query_cmd):
            print('[+]', type, 'zbjl_sx', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, 'Done at', get_current_time())
            continue

        if not os.path.exists('/root/xd_crawler/websocket_data/%s.detail' % webcast_id):
            print('[+]', type, 'zbjl_sx', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, 'Error at', get_current_time())
            continue

        with open('/root/xd_crawler/websocket_data/%s.detail' % webcast_id, 'r') as f:
            detail_data = json.load(f)

        Table_obj = eval('list_' + type + '_zbjl_sx' + '.create()')
        Table_obj.num_zb = one_record.num_zb
        Table_obj.id_zb = one_record.id_zb
        Table_obj.name_zb = one_record.name_zb
        Table_obj.url_zbjl = one_record.url_zbjl
        Table_obj.livestraming_time = one_record.livestraming_time

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
                    '[*] Get zbjl_sx city_url failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl,get_current_time()))
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

        if today_update_flag:
            Table_obj.time_update = get_current_time()
        else:
            Table_obj.time_update = get_current_time() + ' For_History'

        Table_obj.save()

        print('[+]', type, 'zbjl_sx', one_record.num_zb, one_record.name_zb, webcast_id, Table_obj.livestraming_time, 'Done at', get_current_time())

for type in input_type:
    # 日更的数据,必须是zbjl表里属于昨天的直播、且被正确完成日更的数据(即不能是待补抓的数据，也不能是被完成补抓的数据),这些记录才一定会有本地websocket数据
    today_update_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.livestraming_time.startswith('%s'),~list_%s_zbjl.time_update.startswith('First_Established'),~list_%s_zbjl.time_update.endswith('For_History'))" % (type,type,lastday_date, type, type)

    zbjl_sx(today_update_cmd, True)

for type in input_type:
    # 补更的数据,根据zbjl表里标记的数据进行抓取
    crawl_history_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.time_update.endswith('For_History'))" % (type, type)

    zbjl_sx(crawl_history_cmd, False)