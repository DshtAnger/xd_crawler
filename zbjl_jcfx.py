# coding=utf-8

import requests
import json
import datetime
import time
import os,sys
import logging
from DB import *

def get_current_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def calc_time_interval(end_time, start_time):
    return (datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")).seconds

def calc_next_time(livestraming_time, duration):
    return (datetime.datetime.strptime(livestraming_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(minutes=float(duration))).strftime("%Y-%m-%d %H:%M:%S")

def time_comparison(left_time, right_time):
    return datetime.datetime.strptime(left_time, "%Y-%m-%d %H:%M:%S") <= datetime.datetime.strptime(right_time, "%Y-%m-%d %H:%M:%S")

def calc_data(left_time, right_time, left_data, right_data, goal_time):
    return (right_data - left_data)/calc_time_interval(right_time, left_time)*calc_time_interval(goal_time, left_time) + left_data

today_date = datetime.datetime.now().strftime("%Y-%m-%d")
lastday_date = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
first_crawl_date = (datetime.datetime.now()+datetime.timedelta(days=-121)).strftime("%Y-%m-%d")
today_log_dir = '/root/xd_crawler/log/%s' % today_date
if not os.path.exists(today_log_dir):
    os.mkdir(today_log_dir)
logging.basicConfig(format='%(message)s',filename=today_log_dir + '/zbjl_jcfx.log', level=logging.INFO)
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
for current_taks in Entry_list:

    for type in input_type:
        today_zbjl_jcfx_count = 0

        if current_taks == ' Daily ':

            query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.time_update.startswith('%s'),~list_%s_zbjl.time_update.endswith('History'))"%(type, type, today_date, type)

        elif current_taks == 'History':

            query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.time_update.startswith('%s'),list_%s_zbjl.time_update.endswith('History'))"%(type, type, today_date, type)

        for one_record in eval(query_cmd):

            webcast_id = one_record.url_zbjl.split('/')[-1]

            start_time = one_record.livestraming_time
            end_time = calc_next_time(one_record.livestraming_time, one_record.duration)

            trendUser_url = 'https://gw.newrank.cn/api/douyin-webcastdetail/xdnphb/nr/app/douyin/webcastdetail/detail/trendUser'
            trendInteraction_url = 'https://gw.newrank.cn/api/douyin-webcastdetail/xdnphb/nr/app/douyin/webcastdetail/detail/trendInteraction'
            trendBuy_url = 'https://gw.newrank.cn/api/douyin-webcastdetail/xdnphb/nr/app/douyin/webcastdetail/detail/trendBuy'
            trendFansclub_url = 'https://gw.newrank.cn/api/douyin-webcastdetail/xdnphb/nr/app/douyin/webcastdetail/detail/trendFansclub'
            newPromotionEventRecord_url = 'https://gw.newrank.cn/api/douyin-webcastdetail/xdnphb/nr/app/douyin/webcastdetail/detail/newPromotionEventRecord'
            explainPromotionEventRecord_url = 'https://gw.newrank.cn/api/douyin-webcastdetail/xdnphb/nr/app/douyin/webcastdetail/detail/explainPromotionEventRecord'

            promotionList_url = 'https://gw.newrank.cn/api/douyin-webcastdetail/xdnphb/nr/app/douyin/webcastdetail/detail/promotionList'

            post_data = {
                'roomId': webcast_id,
                'startTime': start_time,
                'endTime': end_time
            }

            Retry_times = 10
            continue_next_flag = False
            while 1:
                try:
                    rsp = requests.post(trendUser_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data')
                    trendUser_list = data.get('trend') if isinstance(data,dict) and isinstance(data.get('trend'),list) else []
                except:
                    Retry_times -= 1
                    logging.info('[%s] Get zbjl_jcfx trendUser_url data failed. rsp_code:%s, type:%s, num_zb:%s, url_zbjl:%s at %s' % (current_taks, rsp.status_code, type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                    if Retry_times == 0:
                        continue_next_flag = True
                        break
                    time.sleep(3)
                else:
                    break
            if continue_next_flag:
                logging.info(' '.join(['[%s]' % current_taks, type, 'zbjl_jcfx', one_record.num_zb, one_record.name_zb,'trendUser_url Error at', get_current_time()]))
                continue


            Retry_times = 10
            continue_next_flag = False
            while 1:
                try:
                    rsp = requests.post(trendInteraction_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data')
                    trendInteraction_list = data.get('trend') if isinstance(data,dict) and isinstance(data.get('trend'),list) else []
                except:
                    Retry_times -= 1
                    logging.info('[%s] Get zbjl_jcfx trendInteraction_url data failed. rsp_code:%s, type:%s, num_zb:%s, url_zbjl:%s at %s' % (current_taks, rsp.status_code, type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                    if Retry_times == 0:
                        continue_next_flag = True
                        break
                    time.sleep(3)
                else:
                    break
            if continue_next_flag:
                logging.info(' '.join(['[%s]' % current_taks, type, 'zbjl_jcfx', one_record.num_zb, one_record.name_zb,'trendInteraction_url Error at', get_current_time()]))
                continue


            Retry_times = 10
            continue_next_flag = False
            while 1:
                try:
                    rsp = requests.post(trendBuy_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data')
                    trendBuy_list = data.get('trend') if isinstance(data,dict) and isinstance(data.get('trend'),list) else []
                except:
                    Retry_times -= 1
                    logging.info('[%s] Get zbjl_jcfx trendBuy_url data failed. rsp_code:%s, type:%s, num_zb:%s, url_zbjl:%s at %s' % (current_taks, rsp.status_code, type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                    if Retry_times == 0:
                        continue_next_flag = True
                        break
                    time.sleep(3)
                else:
                    break
            if continue_next_flag:
                logging.info(' '.join(['[%s]' % current_taks, type, 'zbjl_jcfx', one_record.num_zb, one_record.name_zb,'trendBuy_url Error at', get_current_time()]))
                continue


            Retry_times = 10
            continue_next_flag = False
            while 1:
                try:
                    rsp = requests.post(trendFansclub_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data')
                    trendFansclub_list = data.get('trend') if isinstance(data,dict) and isinstance(data.get('trend'),list) else []
                except:
                    Retry_times -= 1
                    logging.info('[%s] Get zbjl_jcfx trendFansclub_url data failed. rsp_code:%s, type:%s, num_zb:%s, url_zbjl:%s at %s' % (current_taks, rsp.status_code, type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                    if Retry_times == 0:
                        continue_next_flag = True
                        break
                    time.sleep(3)
                else:
                    break
            if continue_next_flag:
                logging.info(' '.join(['[%s]' % current_taks, type, 'zbjl_jcfx', one_record.num_zb, one_record.name_zb,'trendFansclub_url Error at', get_current_time()]))
                continue


            Retry_times = 10
            continue_next_flag = False
            while 1:
                try:
                    rsp = requests.post(newPromotionEventRecord_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data')
                    newPromotionEventRecord_list = data.get('record') if isinstance(data,dict) and isinstance(data.get('record'),list) else []
                except:
                    Retry_times -= 1
                    logging.info('[%s] Get zbjl_jcfx newPromotionEventRecord_url data failed. rsp_code:%s, type:%s, num_zb:%s, url_zbjl:%s at %s' % (current_taks, rsp.status_code, type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                    if Retry_times == 0:
                        continue_next_flag = True
                        break
                    time.sleep(3)
                else:
                    break
            if continue_next_flag:
                logging.info(' '.join(['[%s]' % current_taks, type, 'zbjl_jcfx', one_record.num_zb, one_record.name_zb,'newPromotionEventRecord_url Error at', get_current_time()]))
                continue


            Retry_times = 10
            continue_next_flag = False
            while 1:
                try:
                    rsp = requests.post(explainPromotionEventRecord_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data')
                    explainPromotionEventRecord_list = data.get('record') if isinstance(data,dict) and isinstance(data.get('record'),list) else []
                except:
                    Retry_times -= 1
                    logging.info('[%s] Get zbjl_jcfx explainPromotionEventRecord_url data failed. rsp_code:%s, type:%s, num_zb:%s, url_zbjl:%s at %s' % (current_taks, rsp.status_code, type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                    if Retry_times == 0:
                        continue_next_flag = True
                        break
                    time.sleep(3)
                else:
                    break
            if continue_next_flag:
                logging.info(' '.join(['[%s]' % current_taks, type, 'zbjl_jcfx', one_record.num_zb, one_record.name_zb,'explainPromotionEventRecord_url Error at', get_current_time()]))
                continue


            Retry_times = 10
            continue_next_flag = False
            while 1:
                try:
                    post_data = {
                        'roomId': webcast_id
                    }
                    rsp = requests.post(promotionList_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data')
                    promotion_list = data if isinstance(data,list) else []

                    if isinstance(data, str):
                        logging.info('[%s] Get zbjl_jcfx promotionList_url data failed. type:%s, num_zb:%s, url_zbjl:%s, status_code:%s, data:%s, at %s' % (current_taks, type, one_record.num_zb, one_record.url_zbjl, rsp.status_code, data, get_current_time()))

                except:
                    Retry_times -= 1
                    logging.info('[%s] Get zbjl_jcfx promotionList_url data failed. rsp_code:%s, type:%s, num_zb:%s, url_zbjl:%s at %s' % (current_taks, rsp.status_code, type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                    if Retry_times == 0:
                        continue_next_flag = True
                        break
                    time.sleep(3)
                else:
                    break
            if continue_next_flag:
                logging.info(' '.join(['[%s]' % current_taks, type, 'zbjl_jcfx', one_record.num_zb, one_record.name_zb,'promotionList_url Error at', get_current_time()]))
                continue



            current_node_time = calc_next_time(start_time, 1) #'2022-02-18 10:53:54'

            trendUser_list = [{'gmtCreate': current_node_time, 'addUser':0, 'reduceUser':0, 'statsTotalUser':0, 'userCount':0}] + trendUser_list
            trendInteraction_list = [{'gmtCreate': current_node_time, 'statsFollowCount':0,'likeCount':0, 'msgCount':0}] + trendInteraction_list
            trendBuy_list = [{'gmtCreate': current_node_time, 'purchaseCnt':0}] + trendBuy_list
            trendFansclub_list = [{'gmtCreate': current_node_time, 'clubInfoTotalFansCount':0}] + trendFansclub_list


            current_node_time = calc_next_time(start_time, 1)
            next_newPromotion_flag = None
            next_explainPromotion_flag = None
            count = 0
            while time_comparison(current_node_time, end_time):

                Table_obj = eval('list_' + type + '_zbjl_jcfx' + '.create()')
                Table_obj.num_zb = one_record.num_zb
                Table_obj.id_zb = one_record.id_zb
                Table_obj.name_zb = one_record.name_zb
                Table_obj.url_zbjl = one_record.url_zbjl
                Table_obj.livestraming_time = one_record.livestraming_time

                '---------------------------------------------------------------------------------------------------------------------------------------'

                left_node = None
                right_node = None
                for index in range(0,len(trendUser_list)-1):

                    if (time_comparison(trendUser_list[index].get('gmtCreate'), current_node_time) == True) and (time_comparison(trendUser_list[index + 1].get('gmtCreate'), current_node_time) == False):

                        left_node = trendUser_list[index]
                        right_node = trendUser_list[index + 1]
                        break

                if not (left_node and right_node):

                    if len(trendUser_list)>=2 and time_comparison(trendUser_list[-1].get('gmtCreate'),current_node_time):
                        left_node = trendUser_list[-2]
                        right_node = trendUser_list[-1]
                    else:
                        #logging.info('[%s] Get zbjl_jcfx type:%s, num_zb:%s, url_zbjl:%s, trendUser_list current_node_time: %s can not get left or right node at %s' % (current_taks, type, one_record.num_zb, one_record.url_zbjl, current_node_time, get_current_time()))
                        zxrs = '0'
                        ljgk = '0'
                        jcrs = '0'
                        ltrs = '0'
                else:
                    zxrs = '{:.2f}'.format(calc_data(left_node.get('gmtCreate'), right_node.get('gmtCreate'), left_node.get('userCount'), right_node.get('userCount'), current_node_time))
                    ljgk = '{:.2f}'.format(calc_data(left_node.get('gmtCreate'), right_node.get('gmtCreate'), left_node.get('statsTotalUser'), right_node.get('statsTotalUser'), current_node_time))
                    jcrs = '{:.2f}'.format(calc_data(left_node.get('gmtCreate'), right_node.get('gmtCreate'), left_node.get('addUser'), right_node.get('addUser'), current_node_time))
                    ltrs = '{:.2f}'.format(calc_data(left_node.get('gmtCreate'), right_node.get('gmtCreate'), left_node.get('reduceUser'), right_node.get('reduceUser'), current_node_time))


                '---------------------------------------------------------------------------------------------------------------------------------------'


                left_node = None
                right_node = None
                for index in range(0,len(trendInteraction_list)-1):

                    if (time_comparison(trendInteraction_list[index].get('gmtCreate'), current_node_time) == True) and (time_comparison(trendInteraction_list[index + 1].get('gmtCreate'), current_node_time) == False):

                        left_node = trendInteraction_list[index]
                        right_node = trendInteraction_list[index + 1]
                        break

                if not (left_node and right_node):

                    if len(trendInteraction_list)>=2 and  time_comparison(trendInteraction_list[-1].get('gmtCreate'),current_node_time):
                        left_node = trendInteraction_list[-2]
                        right_node = trendInteraction_list[-1]
                    else:
                        #logging.info('[%s] Get zbjl_jcfx type:%s, num_zb:%s, url_zbjl:%s, trendInteraction_list current_node_time: %s can not get left or right node at %s' % (current_taks, type, one_record.num_zb, one_record.url_zbjl, current_node_time, get_current_time()))
                        fss = '0'
                        dzs = '0'
                        dms = '0'
                else:
                    fss = '{:.2f}'.format(calc_data(left_node.get('gmtCreate'), right_node.get('gmtCreate'), left_node.get('statsFollowCount'), right_node.get('statsFollowCount'), current_node_time))
                    dzs = '{:.2f}'.format(calc_data(left_node.get('gmtCreate'), right_node.get('gmtCreate'), left_node.get('likeCount'), right_node.get('likeCount'), current_node_time))
                    dms = '{:.2f}'.format(calc_data(left_node.get('gmtCreate'), right_node.get('gmtCreate'), left_node.get('msgCount'), right_node.get('msgCount'), current_node_time))

                '---------------------------------------------------------------------------------------------------------------------------------------'

                left_node = None
                right_node = None
                for index in range(0,len(trendBuy_list)-1):

                    if (time_comparison(trendBuy_list[index].get('gmtCreate'), current_node_time) == True) and (time_comparison(trendBuy_list[index + 1].get('gmtCreate'), current_node_time) == False):

                        left_node = trendBuy_list[index]
                        right_node = trendBuy_list[index + 1]
                        break

                if not (left_node and right_node):

                    if len(trendBuy_list)>=2 and time_comparison(trendBuy_list[-1].get('gmtCreate'),current_node_time):
                        left_node = trendBuy_list[-2]
                        right_node = trendBuy_list[-1]
                    else:
                        #logging.info('[%s] Get zbjl_jcfx type:%s, num_zb:%s, url_zbjl:%s, trendBuy_list current_node_time: %s can not get left or right node at %s' % (current_taks, type, one_record.num_zb, one_record.url_zbjl, current_node_time, get_current_time()))
                        zzgm = '0'
                else:
                    zzgm = '{:.2f}'.format(calc_data(left_node.get('gmtCreate'), right_node.get('gmtCreate'), left_node.get('purchaseCnt'), right_node.get('purchaseCnt'), current_node_time))


                '---------------------------------------------------------------------------------------------------------------------------------------'

                left_node = None
                right_node = None
                for index in range(0,len(trendFansclub_list)-1):

                    if (time_comparison(trendFansclub_list[index].get('gmtCreate'), current_node_time) == True) and (time_comparison(trendFansclub_list[index + 1].get('gmtCreate'), current_node_time) == False):

                        left_node = trendFansclub_list[index]
                        right_node = trendFansclub_list[index + 1]
                        break

                if not (left_node and right_node):

                    if len(trendFansclub_list)>=2 and time_comparison(trendFansclub_list[-1].get('gmtCreate'),current_node_time):
                        left_node = trendFansclub_list[-2]
                        right_node = trendFansclub_list[-1]
                    else:
                        #logging.info('[%s] Get zbjl_jcfx type:%s, num_zb:%s, url_zbjl:%s, trendFansclub_list current_node_time: %s can not get left or right node at %s' % (current_taks, type, one_record.num_zb, one_record.url_zbjl, current_node_time, get_current_time()))
                        fst = '0'
                else:
                    fst = '{:.2f}'.format(calc_data(left_node.get('gmtCreate'), right_node.get('gmtCreate'), left_node.get('clubInfoTotalFansCount'), right_node.get('clubInfoTotalFansCount'), current_node_time))

                '---------------------------------------------------------------------------------------------------------------------------------------'

                spsj_node = None

                for item in newPromotionEventRecord_list:

                    if next_newPromotion_flag:
                        spsj_node = next_newPromotion_flag
                        next_newPromotion_flag = None
                        break

                    newPromotion_split = item.get('gmtCreate').split(' ')[-1].split(':')

                    current_spilt = current_node_time.split(' ')[-1].split(':')

                    if newPromotion_split[0]==current_spilt[0] and newPromotion_split[1]==current_spilt[1]:
                        if  int(newPromotion_split[2])<=int(current_spilt[2]):
                            spsj_node = item
                            break
                        else:
                            next_newPromotion_flag = item
                            break

                if not spsj_node:
                    spsj_product = 'None'
                    spsj_price = 'None'
                    spsj_sales = 'None'
                else:
                    product_id = None
                    for product in spsj_node.get('record'):
                        if product.get('type') == 1:
                            product_id = product.get('record')[0]
                    if not product_id:
                        spsj_product = 'None'
                        spsj_price = 'None'
                        spsj_sales = 'None'
                    else:
                        product_node = None
                        for promotion in promotion_list:
                            if product_id == promotion.get('productId'):
                                product_node = promotion
                                break
                        if not product_node:
                            spsj_product = 'None'
                            spsj_price = 'None'
                            spsj_sales = 'None'
                        else:
                            spsj_product = product_node.get('title')
                            spsj_price = product_node.get('price')
                            spsj_sales = product_node.get('originAddSales')

                '---------------------------------------------------------------------------------------------------------------------------------------'


                jjsp_node = None

                for item in explainPromotionEventRecord_list:

                    if next_explainPromotion_flag:
                        jjsp_node = next_explainPromotion_flag
                        next_explainPromotion_flag = None
                        break

                    explainPromotion_split = item.get('gmtCreate').split(' ')[-1].split(':')

                    current_spilt = current_node_time.split(' ')[-1].split(':')

                    if explainPromotion_split[0]==current_spilt[0] and explainPromotion_split[1]==current_spilt[1]:
                        if  int(explainPromotion_split[2])<=int(current_spilt[2]):
                            jjsp_node = item
                            break
                        else:
                            next_explainPromotion_flag = item
                            break

                if not jjsp_node:
                    jjsp_product = 'None'
                    jjsp_price = 'None'
                    jjsp_sales = 'None'
                else:
                    product_id = jjsp_node.get('record')[0]

                    product_node = None
                    for promotion in promotion_list:
                        if product_id == promotion.get('productId'):
                            product_node = promotion
                            break
                    if not product_node:
                        jjsp_product = 'None'
                        jjsp_price = 'None'
                        jjsp_sales = 'None'
                    else:
                        jjsp_product = product_node.get('title')
                        jjsp_price = product_node.get('price')
                        jjsp_sales = product_node.get('originAddSales')

                '---------------------------------------------------------------------------------------------------------------------------------------'

                #taffic_time = current_node_time
                #print(taffic_time, zxrs, ljgk, jcrs, ltrs, fss, dzs, dms, zzgm, fst, spsj_product, spsj_price, spsj_sales, '-----------', jjsp_product, jjsp_price, jjsp_sales)

                Table_obj.taffic_time = current_node_time
                Table_obj.zxrs = zxrs
                Table_obj.ljgk = ljgk
                Table_obj.jcrs = jcrs
                Table_obj.ltrs = ltrs
                Table_obj.fss = fss
                Table_obj.dzs = dzs
                Table_obj.dms = dms
                Table_obj.zzgm = zzgm
                Table_obj.fst = fst
                Table_obj.spsj_product = spsj_product
                Table_obj.spsj_price = spsj_price
                Table_obj.spsj_sales = spsj_sales
                Table_obj.jjsp_product = jjsp_product
                Table_obj.jjsp_price = jjsp_price
                Table_obj.jjsp_sales = jjsp_sales


                if current_taks == ' Daily ':
                    Table_obj.time_update = get_current_time()
                elif current_taks == 'History':
                    Table_obj.time_update = get_current_time() + ' History'

                Table_obj.save()
                count += 1
                today_zbjl_jcfx_count += 1

                current_node_time = calc_next_time(current_node_time, 1)

            logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl_jcfx', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, '[ zbjl_jcfx_count: %d ]'%count, 'Done at', get_current_time()]))
        logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl_fcfx', '[ today_zbjl_jcfx_count: %d ]'%today_zbjl_jcfx_count, 'Done at', get_current_time()]))
        logging.info('-'*100)