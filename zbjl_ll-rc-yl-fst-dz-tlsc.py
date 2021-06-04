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


def zbjl_ll(query_cmd, today_update_flag):

    for one_record in eval(query_cmd):

        webcast_id = one_record.url_zbjl.split('/')[-1]

        trend_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/webDetail/trend'
        post_data = {
            "room_id": webcast_id
        }
        while 1:
            try:
                rsp = requests.post(trend_url, headers=headers, data=json.dumps(post_data))
                data_list = json.loads(rsp.text).get('data').get('webcastTrendList')
            except:
                print(
                    '[*] Get zbjl_ll-rc... trend_url failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                time.sleep(5)
            else:
                break

        timepoint_list = []
        for item in data_list:

            crawl_time = item.get('crawl_date')
            timepoint_list.append(crawl_time)

            Table_obj_ll = eval('list_' + type + '_zbjl_ll' + '.create()')
            Table_obj_ll.num_zb = one_record.num_zb
            Table_obj_ll.id_zb = one_record.id_zb
            Table_obj_ll.name_zb = one_record.name_zb
            Table_obj_ll.url_zbjl = one_record.url_zbjl
            Table_obj_ll.livestraming_time = one_record.livestraming_time
            Table_obj_ll.traffic_time = crawl_time
            Table_obj_ll.renshu = item.get('user_count')
            if today_update_flag:
                Table_obj_ll.time_update = get_current_time()
            else:
                Table_obj_ll.time_update = get_current_time() + ' For_History'

            Table_obj_rc = eval('list_' + type + '_zbjl_rc' + '.create()')
            Table_obj_rc.num_zb = one_record.num_zb
            Table_obj_rc.id_zb = one_record.id_zb
            Table_obj_rc.name_zb = one_record.name_zb
            Table_obj_rc.url_zbjl = one_record.url_zbjl
            Table_obj_rc.livestraming_time = one_record.livestraming_time
            Table_obj_rc.watching_time = crawl_time
            Table_obj_rc.leijirenci = item.get('stats_total_user')
            Table_obj_rc.guanzhu = item.get('stats_user_composition_from_my_follow')
            Table_obj_rc.shipintuijian = item.get('stats_user_composition_from_video_detail')
            Table_obj_rc.zhiboguangchang = item.get('stats_user_composition_from_other')
            if today_update_flag:
                Table_obj_rc.time_update = get_current_time()
            else:
                Table_obj_rc.time_update = get_current_time() + ' For_History'

            Table_obj_yl = eval('list_' + type + '_zbjl_yl' + '.create()')
            Table_obj_yl.num_zb = one_record.num_zb
            Table_obj_yl.id_zb = one_record.id_zb
            Table_obj_yl.name_zb = one_record.name_zb
            Table_obj_yl.url_zbjl = one_record.url_zbjl
            Table_obj_yl.livestraming_time = one_record.livestraming_time
            Table_obj_yl.yinlang_time = crawl_time
            Table_obj_yl.yinlang = item.get('stats_fan_ticket')
            if today_update_flag:
                Table_obj_yl.time_update = get_current_time()
            else:
                Table_obj_yl.time_update = get_current_time() + ' For_History'

            Table_obj_fst = eval('list_' + type + '_zbjl_fst' + '.create()')
            Table_obj_fst.num_zb = one_record.num_zb
            Table_obj_fst.id_zb = one_record.id_zb
            Table_obj_fst.name_zb = one_record.name_zb
            Table_obj_fst.url_zbjl = one_record.url_zbjl
            Table_obj_fst.livestraming_time = one_record.livestraming_time
            Table_obj_fst.fans_time = crawl_time
            Table_obj_fst.fans_zb = item.get('club_info_total_fans_count')
            if today_update_flag:
                Table_obj_fst.time_update = get_current_time()
            else:
                Table_obj_fst.time_update = get_current_time() + ' For_History'

            Table_obj_dz = eval('list_' + type + '_zbjl_dz' + '.create()')
            Table_obj_dz.num_zb = one_record.num_zb
            Table_obj_dz.id_zb = one_record.id_zb
            Table_obj_dz.name_zb = one_record.name_zb
            Table_obj_dz.url_zbjl = one_record.url_zbjl
            Table_obj_dz.livestraming_time = one_record.livestraming_time
            Table_obj_dz.support_time = crawl_time
            Table_obj_dz.support_zb = item.get('like_count')
            if today_update_flag:
                Table_obj_dz.time_update = get_current_time()
            else:
                Table_obj_dz.time_update = get_current_time() + ' For_History'

            Table_obj_ll.save()
            Table_obj_rc.save()
            Table_obj_yl.save()
            Table_obj_fst.save()
            Table_obj_dz.save()

        for timepoint in timepoint_list:
            userAvgDuration_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/userAvgDuration'
            post_data = {
                'createTime': one_record.livestraming_time,
                'finishTime': "",
                'startTime': one_record.livestraming_time,
                'endTime': timepoint,
                "roomId": webcast_id
            }
            while 1:
                try:
                    rsp = requests.post(userAvgDuration_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text)
                except:
                    print('[*] Get zbjl_ll-rc... userAvgDuration_url failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                    time.sleep(2)
                else:
                    break

            Table_obj = eval('list_' + type + '_zbjl_tlsc' + '.create()')
            Table_obj.num_zb = one_record.num_zb
            Table_obj.id_zb = one_record.id_zb
            Table_obj.name_zb = one_record.name_zb
            Table_obj.url_zbjl = one_record.url_zbjl
            Table_obj.livestraming_time = one_record.livestraming_time

            Table_obj.timepoint = timepoint
            Table_obj.shichang = str(data.get('data'))
            if today_update_flag:
                Table_obj.time_update = get_current_time()
            else:
                Table_obj.time_update = get_current_time() + ' For_History'

            Table_obj.save()

        print('[+]', type, 'zbjl_ll-rc...', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, 'Done at', get_current_time())


for type in input_type:
    # 日更的数据,必须是zbjl表里属于昨天的直播、且被正确完成日更的数据(即不能是待补抓的数据，也不能是被完成补抓的数据),这些记录才一定会有本地websocket数据
    today_update_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.livestraming_time.startswith('%s'),~list_%s_zbjl.time_update.startswith('First_Established'),~list_%s_zbjl.time_update.endswith('For_History'))" % (type,type,lastday_date, type, type)

    zbjl_ll(today_update_cmd, True)

for type in input_type:
    # 补更的数据,根据zbjl表里标记的数据进行抓取
    crawl_history_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.time_update.endswith('For_History'))" % (type, type)

    zbjl_ll(crawl_history_cmd, False)