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

def zbjl_splb(query_cmd, today_update_flag):

    for one_record in eval(query_cmd):

        webcast_id = one_record.url_zbjl.split('/')[-1]

        with open('/root/xd_crawler/websocket_data/%s.commodity' % webcast_id, 'r') as f:
            commodity_data = json.load(f)

        for product in commodity_data:

            query_cmd = "list_%s_zbjl_splb.select().where(list_%s_zbjl_splb.url_zbjl=='%s',list_%s_zbjl_splb.store_url=='%s',list_%s_zbjl_splb.time_update.startswith('%s'))" % (type, type, one_record.url_zbjl ,type, product.get('detail_url'), type, today_date)
            if eval(query_cmd):
                continue

            Table_obj = eval('list_' + type + '_zbjl_splb' + '.create()')

            Table_obj.num_zb = one_record.num_zb
            Table_obj.id_zb = one_record.id_zb
            Table_obj.name_zb = one_record.name_zb
            Table_obj.url_zbjl = one_record.url_zbjl
            Table_obj.livestraming_time = one_record.livestraming_time

            Table_obj.product_name = product.get('title_total')
            Table_obj.category1 = product.get('promotion_type_v1')
            Table_obj.category2 = product.get('promotion_type_v2')
            Table_obj.store_url = product.get('detail_url')
            Table_obj.yuguyongjin = product.get('cos_fee')
            Table_obj.yongjinratio = product.get('cos_fee_rate')
            Table_obj.zhibojia = product.get('price')
            Table_obj.yuanjian = product.get('market_price')
            Table_obj.quanhoujia = product.get('coupon_price')
            Table_obj.shangjiatime = product.get('create_time')
            Table_obj.shangjiarenshu = product.get('user_count')
            Table_obj.yuguxiaoliang = product.get('add_sales')
            Table_obj.shi = product.get('platform_first_sales')
            Table_obj.mo = product.get('platform_precent_sales')
            Table_obj.yuguxiaoshoue = product.get('sales_money')

            product_id = product.get('product_id')

            staticitem_url = 'https://ec.snssdk.com/product/fxgajaxstaticitem?b_type_new=0&device_id=0&is_outside=1&id={0}&preview=0'.format(product_id)
            while 1:
                try:
                    time.sleep(1)
                    rsp = requests.get(staticitem_url, headers={'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'})
                    # if not json.loads(rsp.text).get('data'):
                    #     raise Exception
                except:
                    print(
                        '[*] Get zbjl_splb staticitem_url failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl,get_current_time()))
                    time.sleep(5)
                else:
                    data = json.loads(rsp.text).get('data')
                    break

            if data == None:
                data = {}

            Table_obj.yishou = data.get('sell_num') if data else '--'
            Table_obj.fahuo_time = data.get('detail_delay_desc') if data else '--'

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
                if 'pay_for_fake' in services:
                    Table_obj.onetothree = '假一赔三'
                if 'customer_protection' in services:
                    Table_obj.protect = '消费者保障服务'
                if 'quick_refund' in services:
                    Table_obj.quick_refund = '极速退'

            pingjia_dic = {'0':'暂无','1': '高', '2': '中', '3': '低'}
            Table_obj.store_name = data.get('shop_name') if data else '--'
            Table_obj.product_experience = str(data.get('credit_score').get('product')) + pingjia_dic[str(data.get('credit_score').get('gap_product'))] if data.get('credit_score') else '--'
            Table_obj.seller_service = str(data.get('credit_score').get('shop')) + pingjia_dic[str(data.get('credit_score').get('gap_shop'))] if data.get('credit_score') else '--'
            Table_obj.shipping_experience = str(data.get('credit_score').get('logistics')) + pingjia_dic[str(data.get('credit_score').get('gap_logistics'))] if data.get('credit_score') else '--'
            Table_obj.tuijianyu = data.get('recommend_remark') if data else '--'

            ajaxitem_url = 'https://ec.snssdk.com/product/ajaxitem?b_type_new=0&device_id=0&is_outside=1&id={0}&abParams=0'.format(product_id)
            while 1:
                try:
                    rsp = requests.get(ajaxitem_url, headers={'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'})
                except:
                    print(
                        '[*] Get zbjl_splb ajaxitem_url failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl,get_current_time()))
                    time.sleep(5)
                else:
                    data = json.loads(rsp.text)
                    break

            # data有可能是[],也可能是{"st":10024,"msg":"商品已下架","data":null}
            if data == [] or data.get('data') == None:
                Table_obj.product_amount = '0'
            else:
                Table_obj.product_amount = str(data.get('data').get('shop_product_count')) if data.get('data') else '--'

            if today_update_flag:
                Table_obj.time_update = get_current_time()
            else:
                Table_obj.time_update = get_current_time() + ' For_History'

            Table_obj.save()

        print('[+]', type, 'zbjl_splb', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, 'Done at', get_current_time())


for type in input_type:
    # 日更的数据,必须是zbjl表里属于昨天的直播、且被正确完成日更的数据(即不能是待补抓的数据，也不能是被完成补抓的数据),这些记录才一定会有本地websocket数据
    today_update_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.livestraming_time.startswith('%s'),~list_%s_zbjl.time_update.startswith('First_Established'),~list_%s_zbjl.time_update.endswith('For_History'))" % (type,type,lastday_date, type, type)

    zbjl_splb(today_update_cmd, True)

for type in input_type:
    # 补更的数据,根据zbjl表里标记的数据进行抓取
    crawl_history_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.time_update.endswith('For_History'))" % (type, type)

    zbjl_splb(crawl_history_cmd, False)

