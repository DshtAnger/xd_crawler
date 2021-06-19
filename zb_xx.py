# coding=utf-8
# need update
# 每天都更新

import requests
import json
import datetime
import time
import os,sys
import logging
from DB import *

def get_current_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

today_date = datetime.datetime.now().strftime("%Y-%m-%d")
today_log_dir = '/root/xd_crawler/log/%s' % today_date
if not os.path.exists(today_log_dir):
    os.mkdir(today_log_dir)
logging.basicConfig(format='%(message)s',filename=today_log_dir + '/zb_xx.log', level=logging.INFO)
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
    today_zbxx_count = 0

    for one_record in eval('list_%s.select().where(list_%s.time_update=="%s")'%(type, type, today_date)):

        query_cmd = "list_%s_zbxx.select().where(list_%s_zbxx.url_zb=='%s',list_%s_zbxx.time_update.startswith('%s'))" % (type, type, one_record.url_zb, type, today_date)
        if eval(query_cmd):
            logging.info(' '.join(['[+]', type, 'zb_zbxx', one_record.num_zb, one_record.name_zb, 'This is Repeated data. Continue next at', get_current_time()]))
            continue

        # query_cmd = "list_%s_zbxx.select().where(list_%s_zbxx.url_zb=='%s',list_%s_zbxx.time_update.endswith('Error has occurred'))" % (type, type, one_record.url_zb, type)
        # eval_result = eval(query_cmd)
        # if eval_result:
        #     Table_obj = eval_result[0]
        # else:
        #     pass


        uid = one_record.url_zb.split('/')[-1]
        account_info_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/detail/accountInfoAll'
        post_data = {
            'uid': uid
        }

        Retry_times = 10
        continue_next_flag = False
        while 1:
            try:
                rsp = requests.post(account_info_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data')
            except:
                Retry_times -= 1
                logging.info('[*] Get zbxx account_info_url failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, get_current_time()))
                if Retry_times == 0:
                    continue_next_flag = True
                    break
                time.sleep(5)
            else:
                break
        if continue_next_flag:
            #Table_obj.time_update = '[%s] account_info_url Error has occurred' % get_current_time()
            #Table_obj.save()
            continue

        one_record.id_zb = data.get('account')
        one_record.name_zb = data.get('nickname')
        one_record.save()

        Table_obj = eval('list_' + type + '_zbxx' + '.create()')
        Table_obj.num_zb = one_record.num_zb
        Table_obj.url_zb = one_record.url_zb

        Table_obj.id_zb = data.get('account')
        Table_obj.name_zb = data.get('nickname')
        Table_obj.index = data.get('newrank_index')
        Table_obj.categoly = data.get('type')
        Table_obj.rank = data.get('rank_position')
        Table_obj.label = '/'.join(data.get('ana_xd_tags'))

        custom_verify = data.get('custom_verify')
        enterprise_verify = data.get('enterprise_verify_reason')
        if '' in [custom_verify,enterprise_verify]:
            Table_obj.certification = custom_verify + enterprise_verify
        else:
            Table_obj.certification = custom_verify + '/' + enterprise_verify

        Table_obj.gender = data.get('gender')

        province = data.get('province')
        city = data.get('city')
        if province == '':
            Table_obj.origin = city
        elif province and city:
            Table_obj.origin = province + "·" + city

        Table_obj.age = data.get('age')
        Table_obj.xingzuo = data.get('constellation_name')
        Table_obj.school = data.get('school_name')
        Table_obj.introduction = data.get('signature')
        Table_obj.time_sl = data.get('collection_time')
        Table_obj.notice = data.get('scheduled_time_text')
        Table_obj.fans = data.get('mplatform_followers_count')
        Table_obj.works = data.get('aweme_count')
        Table_obj.supports = data.get('total_favorited')
        Table_obj.zfb = data.get('favorited_follower_rate')
        Table_obj.dhb = data.get('aweme_promotion_rate')
        Table_obj.zcb = data.get('seed_aweme_rate')
        Table_obj.MCN = data.get('mcn_name') if data.get('mcn_name') else '--'
        _mcn_id = data.get('mcn_id')

        '--------------------------------------------------------------------------------------------'

        xingtu_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/detail/xingtuIndex'
        post_data = {
            'uid':one_record.id_zb
        }

        Retry_times = 10
        continue_next_flag = False
        while 1:
            try:
                rsp = requests.post(xingtu_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data')
            except:
                Retry_times -= 1
                logging.info('[*] Get zb_zbxx xingtu_url failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, get_current_time()))
                if Retry_times == 0:
                    continue_next_flag = True
                    break
                time.sleep(5)
            else:
                break
        if continue_next_flag:
            Table_obj.time_update = '[%s] xingtu_url Error has occurred' % get_current_time()
            Table_obj.save()
            continue

        if data:
            Table_obj.drzs = data.get('star_score')
            Table_obj.hezuo = data.get('cooperation_score')
            Table_obj.xingjiabi = data.get('cp_score')
            Table_obj.zhangfen = data.get('growth_score')
            Table_obj.zhongcao = data.get('shop_score')
            Table_obj.chuanbo = data.get('spread_score')
        else:
            Table_obj.drzs, Table_obj.hezuo, Table_obj.xingjiabi, Table_obj.zhangfen, Table_obj.zhongcao, Table_obj.chuanbo = ["--"] * 6

        '--------------------------------------------------------------------------------------------'

        if _mcn_id:

            mcn_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/mcn/detail'
            post_data = {
                'mcn_id': _mcn_id
            }

            Retry_times = 10
            continue_next_flag = False
            while 1:
                try:
                    rsp = requests.post(mcn_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data')
                except:
                    Retry_times -= 1 
                    logging.info('[*] Get zb_zbxx mcn_url failed. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb, get_current_time()))
                    if Retry_times == 0:
                        continue_next_flag = True
                        break
                    time.sleep(5)
                else:
                    break
            if continue_next_flag:
                Table_obj.time_update = '[%s] mcn_url Error has occurred' % get_current_time()
                Table_obj.save()
                continue

            Table_obj.qianyue = data.get('user_count') if data else '--'
            Table_obj.origin_mcn = data.get('city')  if data else '--'
        else:
            Table_obj.qianyue, Table_obj.origin_mcn = ["--"] * 2

        Table_obj.time_update = get_current_time()
        Table_obj.save()
        today_zbxx_count += 1

        logging.info(' '.join(['[+]', type, 'zb_zbxx', one_record.num_zb, one_record.name_zb, 'Done at', get_current_time()]))
    else:
        logging.info(' '.join(['[+]', type, 'zb_zbxx', '[ today_zbxx_count: %d ]'%today_zbxx_count, 'Done at', get_current_time()]))
        logging.info('-' * 100)