# coding=utf-8
# 首日启动,什么都不做,轮空
# 次日启动,每天抓取昨天的相应数据(如6月1日抓取5月31日的)

import requests
import json
import datetime
import time
import random
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
logging.basicConfig(format='%(message)s',filename=today_log_dir + '/zbjl_splb.log', level=logging.INFO)
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.info("--------------------Uncaught Exception--------------------",exc_info=(exc_type, exc_value, exc_traceback))
sys.excepthook = handle_exception

REPEAT_CHECK_FLAG = False

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

item2_page_headers = {
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"',
    'sec-ch-ua-mobile': '?0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-User': '?1',
    'Sec-Fetch-Dest': 'document',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'If-None-Match': 'W/"60bdcdb4-50f8"',
    'If-Modified-Since': 'Mon, 07 Jun 2021 07:41:40 GMT',
}

pseudo_header = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'zh-CN,zh;q=0.9',
    'origin': 'https://haohuo.jinritemai.com',
    'referer': 'https://haohuo.jinritemai.com/',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': "macOS",
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36'
}

Entry_list = {
    ' Daily ': True,
    'History': True
}
# session = requests.Session()
for current_taks in Entry_list:

    for type in input_type:
        today_splb_count = 0

        if current_taks == ' Daily ':

            query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.time_update.startswith('%s'),~list_%s_zbjl.time_update.endswith('History'))"%(type, type, today_date, type)

        elif current_taks == 'History':

            query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.time_update.startswith('%s'),list_%s_zbjl.time_update.endswith('History'))"%(type, type, today_date, type)

        for one_record in eval(query_cmd):

            webcast_id = one_record.url_zbjl.split('/')[-1]

            promotionList_url = 'https://gw.newrank.cn/api/douyin-webcastdetail/xdnphb/nr/app/douyin/webcastdetail/detail/promotionList'
            post_data = {
                'roomId': webcast_id
            }
            Retry_times = 10
            continue_next_flag = False
            while 1:
                try:
                    rsp = requests.post(promotionList_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data')
                    promotion_data = data if isinstance(data, list) else []

                    if isinstance(data, str):
                        logging.info('[%s] Get zbjl_splb promotionList_url data failed. type:%s, num_zb:%s, url_zbjl:%s, status_code:%s, data:%s at %s' % (current_taks, type, one_record.num_zb, one_record.url_zbjl, rsp.status_code, data, get_current_time()))

                except:
                    Retry_times -= 1
                    logging.info('[%s] Get zbjl_splb promotionList_url data failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (current_taks, type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                    if Retry_times == 0:
                        continue_next_flag = True
                        break
                    time.sleep(5)
                else:
                    break
            if continue_next_flag:
                logging.info(' '.join(['[%s]' % current_taks, type, 'zbjl_splb', one_record.num_zb, one_record.name_zb,'promotionList_url Error at', get_current_time()]))
                continue


            splb_count = 0
            for product in promotion_data:

                if not isinstance(product, dict):
                    continue

                if REPEAT_CHECK_FLAG:
                    repeat_detect_cmd = "list_%s_zbjl_splb.select().where(list_%s_zbjl_splb.url_zbjl=='%s',list_%s_zbjl_splb.store_url=='%s',list_%s_zbjl_splb.time_update.startswith('%s'))" % (type, type, one_record.url_zbjl ,type, product.get('detail_url'), type, today_date)
                    if eval(repeat_detect_cmd):
                        logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl_splb', one_record.num_zb, one_record.name_zb, webcast_id,one_record.livestraming_time, 'product_id:%s'%product.get('product_id'), 'This is Repeated data. Continue next at', get_current_time()]))
                        continue

                Table_obj = eval('list_' + type + '_zbjl_splb' + '.create()')

                Table_obj.num_zb = one_record.num_zb
                Table_obj.id_zb = one_record.id_zb
                Table_obj.name_zb = one_record.name_zb
                Table_obj.url_zbjl = one_record.url_zbjl
                Table_obj.livestraming_time = one_record.livestraming_time

                Table_obj.product_name = product.get('title')
                Table_obj.category1 = product.get('promotionTypePredict')
                Table_obj.category2 = product.get('promotionTypeV2')
                Table_obj.store_url = 'https:' + product.get('detailUrl')

                Table_obj.yuguyongjin = product.get('cosFee')
                Table_obj.yongjinratio = product.get('cosFeeRate')
                Table_obj.zhibojia = product.get('price')
                Table_obj.yuanjian = product.get('marketPrice')
                Table_obj.quanhoujia = product.get('couponPrice')
                Table_obj.shangjiatime = product.get('createTime')
                Table_obj.shangjiarenshu = product.get('userCount')
                Table_obj.yuguxiaoliang = product.get('addSales')
                Table_obj.shi = product.get('firstSales')
                Table_obj.mo = product.get('lastSales')
                Table_obj.yuguxiaoshoue = product.get('salesMoney')

                # item2_url = product.get('detail_url')
                # session.get(url=item2_url, headers=item2_page_headers)

                product_id = product.get('productId')

                WAIT_TIME = random.choice([0.5, 0.6, 0.7, 0.8])

                # get_msToken_url = 'https://ec.snssdk.com/index/getNewAbUiParams?token=aweme&b_type_new=0&device_id=0&is_outside=1'
                # rsp = requests.get(get_msToken_url, headers=pseudo_header)
                # msToken = rsp.headers.get('Set-Cookie', '').split(';')[0]
                # pseudo_header.update({'cookie':msToken})

                staticitem_url = 'https://ec.snssdk.com/product/fxgajaxstaticitem?id={0}'.format(product_id)
                # staticitem_url = 'https://ec.snssdk.com/product/fxgajaxstaticitem?b_type_new=0&device_id=0&is_outside=1&id={0}&preview=0&is_native_h5=1'.format(product_id)
                Retry_times = 3
                continue_next_flag = False
                while 1:
                    data = None
                    try:
                        time.sleep(WAIT_TIME)
                        # rsp = requests.get(staticitem_url, headers={'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'})
                        # rsp = session.get(url=staticitem_url, headers=pseudo_header)
                        rsp = requests.get(staticitem_url, headers=pseudo_header)
                        if rsp.text == '':
                            data = None
                        else:
                            data = json.loads(rsp.text).get('data')
                            data = data if isinstance(data,dict) else {}
                    except:
                        Retry_times -= 1
                        logging.info('[*] Get zbjl_splb staticitem_url failed. type:%s, num_zb:%s, url_zbjl:%s product_id:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl, product_id, get_current_time()))
                        if Retry_times == 0:
                            continue_next_flag = True
                            break
                        time.sleep(5)
                    else:
                        break
                if continue_next_flag:
                    Table_obj.time_update = '%s staticitem_url Error' % get_current_time()
                    Table_obj.save()
                    continue

                if data == None:
                    data = {}

                Table_obj.yishou = data.get('sell_num') if data else '--'

                if data:
                    Table_obj.fahuo_time = data.get('detail_delay_desc') if data.get('detail_delay_desc') else data.get('sku_presell_delay_desc')
                    if Table_obj.fahuo_time == "":
                        Table_obj.fahuo_time = data.get('sku_delay_desc')
                else:
                    Table_obj.fahuo_time = '--'

                if data.get('freight'):
                    Table_obj.fahuo_city = data.get('freight').get('product_province_name') if data.get('freight') else '--'
                    Table_obj.shippingfee = '不确定' if data else '--'
                else:
                    Table_obj.fahuo_city = '--'
                    Table_obj.shippingfee = '包邮' if data else '--'

                Table_obj.sevendays, Table_obj.onetothree, Table_obj.protect, Table_obj.quick_refund = ['--'] * 4
                services = data.get('product_tag').get('service_tag') if data.get('product_tag') else '--'
                if services:
                    if 'support_7days_refund' in services:
                        Table_obj.sevendays = '7天无理由退货'
                    if 'unsupport_7days_refund' in services:
                        Table_obj.sevendays = '不支持7天无理由退货'
                    if 'pay_for_bad' in services:
                        Table_obj.onetothree = '假一赔三'
                    if 'customer_protection' in services:
                        Table_obj.protect = '消费者保障服务'
                    if 'quick_refund' in services:
                        Table_obj.quick_refund = '极速退'

                pingjia_dic = {'0':'暂无','1': '高', '2': '中', '3': '低'}
                Table_obj.store_name = data.get('shop_name') if data else '--'
                Table_obj.product_experience = str(data.get('credit_score').get('product')) + str(pingjia_dic.get(str(data.get('credit_score').get('gap_product')))) if data.get('credit_score') else '--'
                Table_obj.seller_service = str(data.get('credit_score').get('shop')) + str(pingjia_dic.get(str(data.get('credit_score').get('gap_shop')))) if data.get('credit_score') else '--'
                Table_obj.shipping_experience = str(data.get('credit_score').get('logistics')) + str(pingjia_dic.get(str(data.get('credit_score').get('gap_logistics')))) if data.get('credit_score') else '--'
                Table_obj.tuijianyu = data.get('recommend_remark') if data else '--'

                # ajaxitem_url = 'https://ec.snssdk.com/product/ajaxitem?b_type_new=0&device_id=0&is_outside=1&id={0}&abParams=0'.format(product_id)
                # ajaxitem_url = 'https://ec.snssdk.com/product/ajaxitem?id={0}'.format(product_id)
                #
                # while 1:
                #     try:
                #         # time.sleep(WAIT_TIME)
                #         # rsp = requests.get(ajaxitem_url, headers={'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'})
                #         rsp = requests.get(ajaxitem_url, headers=pseudo_header)
                #         # rsp = session.get(url=ajaxitem_url, headers=pseudo_header)
                #         data = json.loads(rsp.text)
                #     except:
                #         logging.info('[*] Get zbjl_splb ajaxitem_url failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                #         time.sleep(5)
                #     else:
                #         break

                data = []

                if data == []:
                    # data得不到数据,因为访问频率的问题,值是[]
                    Table_obj.product_amount = '--'
                else:
                    # data能取到数据,值是{"st":10024,"msg":"商品已下架","data":null}
                    if data.get('data') == None and data.get('msg') == "商品已下架":
                        Table_obj.yishou = data.get('msg')
                        Table_obj.shippingfee = '--'
                        Table_obj.fahuo_time = '--'
                    Table_obj.product_amount = str(data.get('data').get('shop_product_count')) if data.get('data') else '--'

                if current_taks == ' Daily ':
                    Table_obj.time_update = get_current_time()
                elif current_taks == 'History':
                    Table_obj.time_update = get_current_time() + ' History'

                Table_obj.save()
                splb_count += 1
                today_splb_count += 1
                logging.info(' '.join(['[%s]' % current_taks, type, 'zbjl_splb', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, 'product_id:%s'%product_id, 'Done at', get_current_time()]))
            logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl_splb', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, '[ splb_count: %d ]'%splb_count, 'Done at', get_current_time()]))
            logging.info('-'*50)
        logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl_splb', '[ today_splb_count: %d ]'%today_splb_count, 'Done at', get_current_time()]))
        logging.info('-'*100)

#os.system('python3 /root/xd_crawler/logout.py')
#logging.info('[+] Today cookie: %s logout Done at %s'%(cookie,get_current_time()))