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

Entry_list = {
    ' Daily ': True,
    'History': True
}
for current_taks in Entry_list:

    for type in input_type:

        if current_taks == ' Daily ':
            update_date = lastday_date

        elif current_taks == 'History':
            query_cmd = "list_%s_zbjl_splb.select().where(list_%s_zbjl_splb.time_update.endswith('History')).order_by(list_%s_zbjl_splb.time_update).limit(10)" % (type, type, type)
            query_result = eval(query_cmd)

            if bool(query_result):
                latest_history_date = query_result[0].time_update.split(' ')[0]
            else:
                # 首日运行没有带标记的历史数据情景
                latest_history_date = lastday_date

            update_date = (datetime.datetime.strptime(latest_history_date, "%Y-%m-%d") + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")

        query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.livestraming_time.startswith('%s'))" % (type, type, update_date)

        for one_record in eval(query_cmd):

            webcast_id = one_record.url_zbjl.split('/')[-1]

            if not os.path.exists('/root/xd_crawler/websocket_data/%s.detail' % webcast_id):
                Table_obj = eval('list_' + type + '_zbjl_splb' + '.create()')
                Table_obj.num_zb = one_record.num_zb
                Table_obj.id_zb = one_record.id_zb
                Table_obj.name_zb = one_record.name_zb
                Table_obj.url_zbjl = one_record.url_zbjl
                Table_obj.livestraming_time = one_record.livestraming_time
                Table_obj.time_update = 'Severe error occurred at %s' % get_current_time()
                Table_obj.save()
                print('[%s]'%current_taks, type, 'zbjl_splb', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, 'Find local websocket file failed  at', get_current_time())
                continue

            with open('/root/xd_crawler/websocket_data/%s.commodity' % webcast_id, 'r') as f:
                commodity_data = json.load(f)

            splb_count = 0
            for product in commodity_data:

                repeat_detect_cmd = "list_%s_zbjl_splb.select().where(list_%s_zbjl_splb.url_zbjl=='%s',list_%s_zbjl_splb.store_url=='%s',list_%s_zbjl_splb.time_update.startswith('%s'))" % (type, type, one_record.url_zbjl ,type, product.get('detail_url'), type, today_date)
                if eval(repeat_detect_cmd):
                    print('[%s]'%current_taks, type, 'zbjl_splb', one_record.num_zb, one_record.name_zb, webcast_id,one_record.livestraming_time, 'This is Repeated data. Continue next at', get_current_time())
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
                        rsp = requests.get(staticitem_url, headers={'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'})
                    except:
                        print('[*] Get zbjl_splb staticitem_url failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
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
                        print('[*] Get zbjl_splb ajaxitem_url failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                        time.sleep(5)
                    else:
                        data = json.loads(rsp.text)
                        break

                # data有可能是[],也可能是{"st":10024,"msg":"商品已下架","data":null}
                if data == [] or data.get('data') == None:
                    Table_obj.product_amount = '0'
                else:
                    Table_obj.product_amount = str(data.get('data').get('shop_product_count')) if data.get('data') else '--'

                if current_taks == ' Daily ':
                    Table_obj.time_update = get_current_time()
                elif current_taks == 'History':
                    Table_obj.time_update = get_current_time() + ' History'

                Table_obj.save()
                splb_count += 1

            print('[%s]'%current_taks, type, 'zbjl_splb', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, '[ zbjl_splb amount: %d ]'%splb_count, 'Done at', get_current_time())