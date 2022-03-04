# coding=utf-8
# no need update
# 只是最初建立，以后不更新

import requests
import json
import datetime,time
import pandas
import os,sys
import logging
from DB import *

def get_current_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

today_date = datetime.datetime.now().strftime("%Y-%m-%d")
lastday_date = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")

today_log_dir = '/root/xd_crawler/log/%s' % today_date
if not os.path.exists(today_log_dir):
    os.mkdir(today_log_dir)
logging.basicConfig(format='%(message)s',filename=today_log_dir + '/zb_rootdir.log', level=logging.INFO)
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

    if len(eval('list_' + type + '.select()')) == 0:
        #初次建立场景,使用excel建立

        data_sheet = pandas.read_excel('/root/xd_crawler/download_excel/%s.xlsx'%type, sheet_name='基础数据' )
        douyin_id_list = list(data_sheet['抖音号ID'])
        douyin_name_list = list(data_sheet['抖音号名称'])
        douyin_identify_list = list(data_sheet['认证'])

        for id_zb in douyin_id_list:

            post_data = {
                'keyword': id_zb
            }

            id_search_error_tag = False
            name_search_error_tag = False
            identify_search_error_tag = False

            num_zb = str(douyin_id_list.index(id_zb) + 1) + type
            name_zb = douyin_name_list[douyin_id_list.index(id_zb)]
            while 1:
                try:
                    searchAccount_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/searchAccountList'
                    rsp = requests.post(searchAccount_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data').get('list')
                    if len(data) == 0 and (not id_search_error_tag):
                        id_search_error_tag = True
                        raise Exception
                    if len(data) == 0 and (not name_search_error_tag):
                        name_search_error_tag = True
                        raise Exception
                    if len(data) == 0 and (not identify_search_error_tag):
                        identify_search_error_tag = True
                        raise Exception
                except:
                    if id_search_error_tag and (not name_search_error_tag):
                        search_name = douyin_name_list[douyin_id_list.index(id_zb)]
                        if '（' in search_name:
                            search_name = search_name.split('（')[0]
                        if '（' in search_name:
                            search_name = search_name.split('【')[0]
                        if '?' in search_name:
                            search_name = search_name.split('?')[0]
                        post_data.update({'keyword': search_name})

                    if name_search_error_tag:
                        search_identify = douyin_identify_list[douyin_id_list.index(id_zb)]
                        post_data.update({'keyword': search_identify})

                        if str(search_identify) == 'nan':
                            identify_search_error_tag = True

                    logging.info('[*] Get zb_rootdir failed. %s id_zb:%s name_zb:%s id_search_error_tag:%s name_search_error_tag:%s identify_search_error_tag:%s' % (num_zb, id_zb, name_zb, id_search_error_tag, name_search_error_tag, identify_search_error_tag))

                    if identify_search_error_tag:
                        break
                    else:
                        time.sleep(5)
                else:
                    break

            if id_search_error_tag and name_search_error_tag and identify_search_error_tag:
                logging.info('[*] Can not find this: %s %s %s. To continux next.'%(num_zb, id_zb, name_zb))
                continue

            one_data = None
            if len(data) > 1:
                #主播名匹配
                for item in data:
                    if item.get('nickname')[:3] in name_zb:
                        one_data = item
                        break

                if one_data == None:
                    #粉丝数匹配
                    max_followers = max([int(i.get('mplatform_followers_count')) for i in data])
                    for item in data:
                        if int(item.get('mplatform_followers_count')) == max_followers:
                            one_data = item
                            break
            else:
                one_data = data[0]

            url_zb = 'https://xd.newrank.cn/tiktok/detailV2/overview/%s' % one_data.get('uid')
            #首次建立不写入id_zb,待zbxx运行后再更新id_zb和name_zb

            eval('list_' + type + '.create(num_zb=num_zb, id_zb=id_zb, name_zb=name_zb, url_zb=url_zb, time_update=today_date)')

            logging.info("[+] %s %s Done at %s" % (num_zb, name_zb, today_date))
        logging.info("-" * 50)

    else:
        # 日更场景
        # 数据库里最新最近的日期
        query_cmd = 'list_%s.select(list_%s.time_update).order_by(-list_%s.time_update)' % (type, type, type)
        newest_date = eval(query_cmd)[0].time_update

        lastday_date = newest_date
        query_cmd = 'list_%s.select().where(list_%s.time_update=="%s")'%(type,type,lastday_date)
        for one_record in eval(query_cmd):

            num_zb = one_record.num_zb
            id_zb = one_record.id_zb
            name_zb = one_record.name_zb

            post_data = {
                'keyword': id_zb
            }
            id_search_error_tag = False
            name_search_error_tag = False
            while 1:
                try:
                    searchAccount_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/searchAccountList'
                    rsp = requests.post(searchAccount_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data').get('list')
                    if len(data) == 0 and (not id_search_error_tag):
                        id_search_error_tag = True
                        raise Exception
                    if len(data) == 0 and (not name_search_error_tag):
                        name_search_error_tag = True
                        raise Exception
                except:
                    if id_search_error_tag and (not name_search_error_tag):
                        search_name = name_zb
                        if '（' in search_name:
                            search_name = search_name.split('（')[0]
                        if '（' in search_name:
                            search_name = search_name.split('【')[0]
                        if '?' in search_name:
                            search_name = search_name.replace('?','')
                        post_data.update({'keyword': search_name})

                    logging.info('[*] Get zb_rootdir failed. %s id_zb:%s name_zb:%s id_search_error_tag:%s name_search_error_tag:%s' % (num_zb, id_zb, name_zb, id_search_error_tag, name_search_error_tag))

                    if id_search_error_tag and name_search_error_tag:
                        break
                    else:
                        time.sleep(5)
                else:
                    break
            if id_search_error_tag and name_search_error_tag:
                logging.info('[*] Can not find new this: %s %s %s. To continux next.' % (num_zb, id_zb, name_zb))
                continue

            one_data = None
            if len(data) > 1:
                # 主播名匹配
                for item in data:
                    if item.get('nickname')[:3] in name_zb:
                        one_data = item
                        break

                if one_data == None:
                    # 粉丝数匹配
                    max_followers = max([int(i.get('mplatform_followers_count')) for i in data])
                    for item in data:
                        if int(item.get('mplatform_followers_count')) == max_followers:
                            one_data = item
                            break
            else:
                one_data = data[0]

            url_zb = 'https://xd.newrank.cn/tiktok/detailV2/overview/%s' % one_data.get('uid')
            # 首次建立不写入id_zb,待zbxx运行后再更新id_zb和name_zb

            eval('list_' + type + '.create(num_zb=num_zb, id_zb=id_zb, name_zb=name_zb, url_zb=url_zb, time_update=today_date)')
            logging.info("[+] %s %s Done at %s" % (num_zb, name_zb, today_date))
        logging.info("-" * 100)



'''for type in input_type:

    zb_index = 1
    post_data = {
        'account_type': type_list[type],
        'date_type': "2",
        'rank_date': last_month_last_day,
        'size': '50',
        'start': ''
    }

    for page in range(1, 5):

        post_data.update({'start': page})

        while 1:
            try:
                saleRank_search_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/webcastSalesRank'
                rsp = requests.post(saleRank_search_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data').get('list')
            except:
                logging.info('[*] Get zb rootdir type:%s page:%d failed...Retrying..'%(type,page))
                time.sleep(5)
            else:
                break

        for item in data:
            num_zb = '{index}{type}'.format(index=zb_index, type=type)
            id_zb = item.get('unique_id')
            name_zb = item.get('user_nickname')
            url_zb = 'https://xd.newrank.cn/tiktok/detail/latest/%s' % item.get('uid')

            #single_list.append([num_zb,id_zb,name_zb,url_zb])
            eval('list_' + type + '.create(num_zb=num_zb, id_zb=id_zb, name_zb=name_zb, url_zb=url_zb)')
            zb_index += 1

        logging.info("[+] %s page %d Done."%(type,page))
    logging.info("[+] %s page %d Done." % (type, page))'''