# coding=utf-8
# 首日启动,什么都不做,轮空
# 次日启动,每天抓取昨天的相应数据(如6月1日抓取5月31日的)

# 尝试多进程抓取,解决旧数据库写法无法在多进程场景实用问题.但受制于staticitem_url接口访问频率限制,yishou后字段太多无法获取到,故废弃

import requests
import websocket
import json
import datetime
import time
import os,sys
import logging
#from DB import *
from multiprocessing import Pool

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
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"',
    'sec-ch-ua-mobile': '?0',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
}

Entry_list = {
    ' Daily ': True,
    'History': True
}
# session = requests.Session()
# for current_taks in Entry_list:

def run_crawler_task(type, current_taks):

    #for type in input_type:

    from peewee import MySQLDatabase
    from peewee import Model, CharField

    db1 = MySQLDatabase('xd', user='root', password='Wanghongpeng1', host='127.0.0.1', port=3306)  # ,charset='utf8mb4')
    db1.connection()

    class BaseModel(Model):
        class Meta:
            database = db1

    class list_ms_zbjl(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)
        theme = CharField(max_length=256, null=False)

        duration = CharField(max_length=256, null=False)
        yinlangshouru = CharField(max_length=256, null=False)
        benchangyinlang = CharField(max_length=256, null=False)
        songli = CharField(max_length=256, null=False)
        zaixianfengzhi = CharField(max_length=256, null=False)
        leijiguankan = CharField(max_length=256, null=False)
        zongdianzan = CharField(max_length=256, null=False)
        danchangzhangfen = CharField(max_length=256, null=False)
        zhuanfenlv = CharField(max_length=256, null=False)
        pingjunzaixian = CharField(max_length=256, null=False)
        pingjunzhiliu = CharField(max_length=256, null=False)
        yuguxiaoshoue = CharField(max_length=256, null=False)
        yuguxiaoshouliang = CharField(max_length=256, null=False)
        shangjiashangpin = CharField(max_length=256, null=False)
        zuigaodanjia = CharField(max_length=256, null=False)
        zuigaoxiaoliang = CharField(max_length=256, null=False)
        zuigaoxiaoshoue = CharField(max_length=256, null=False)
        kedanjia = CharField(max_length=256, null=False)
        renjungoumaijiazhi = CharField(max_length=256, null=False)
        xiaoshouzhuanhualv = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_ss_zbjl(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)
        theme = CharField(max_length=256, null=False)

        duration = CharField(max_length=256, null=False)
        yinlangshouru = CharField(max_length=256, null=False)
        benchangyinlang = CharField(max_length=256, null=False)
        songli = CharField(max_length=256, null=False)
        zaixianfengzhi = CharField(max_length=256, null=False)
        leijiguankan = CharField(max_length=256, null=False)
        zongdianzan = CharField(max_length=256, null=False)
        danchangzhangfen = CharField(max_length=256, null=False)
        zhuanfenlv = CharField(max_length=256, null=False)
        pingjunzaixian = CharField(max_length=256, null=False)
        pingjunzhiliu = CharField(max_length=256, null=False)
        yuguxiaoshoue = CharField(max_length=256, null=False)
        yuguxiaoshouliang = CharField(max_length=256, null=False)
        shangjiashangpin = CharField(max_length=256, null=False)
        zuigaodanjia = CharField(max_length=256, null=False)
        zuigaoxiaoliang = CharField(max_length=256, null=False)
        zuigaoxiaoshoue = CharField(max_length=256, null=False)
        kedanjia = CharField(max_length=256, null=False)
        renjungoumaijiazhi = CharField(max_length=256, null=False)
        xiaoshouzhuanhualv = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_kj_zbjl(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)
        theme = CharField(max_length=256, null=False)

        duration = CharField(max_length=256, null=False)
        yinlangshouru = CharField(max_length=256, null=False)
        benchangyinlang = CharField(max_length=256, null=False)
        songli = CharField(max_length=256, null=False)
        zaixianfengzhi = CharField(max_length=256, null=False)
        leijiguankan = CharField(max_length=256, null=False)
        zongdianzan = CharField(max_length=256, null=False)
        danchangzhangfen = CharField(max_length=256, null=False)
        zhuanfenlv = CharField(max_length=256, null=False)
        pingjunzaixian = CharField(max_length=256, null=False)
        pingjunzhiliu = CharField(max_length=256, null=False)
        yuguxiaoshoue = CharField(max_length=256, null=False)
        yuguxiaoshouliang = CharField(max_length=256, null=False)
        shangjiashangpin = CharField(max_length=256, null=False)
        zuigaodanjia = CharField(max_length=256, null=False)
        zuigaoxiaoliang = CharField(max_length=256, null=False)
        zuigaoxiaoshoue = CharField(max_length=256, null=False)
        kedanjia = CharField(max_length=256, null=False)
        renjungoumaijiazhi = CharField(max_length=256, null=False)
        xiaoshouzhuanhualv = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_ms_zbjl_splb(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)

        product_name = CharField(max_length=256, null=False)
        category1 = CharField(max_length=256, null=False)
        category2 = CharField(max_length=256, null=False)
        store_url = CharField(max_length=1024, null=False)
        yuguyongjin = CharField(max_length=256, null=False)
        yongjinratio = CharField(max_length=256, null=False)
        zhibojia = CharField(max_length=256, null=False)
        yuanjian = CharField(max_length=256, null=False)
        quanhoujia = CharField(max_length=256, null=False)
        shangjiatime = CharField(max_length=256, null=False)
        shangjiarenshu = CharField(max_length=256, null=False)
        yuguxiaoliang = CharField(max_length=256, null=False)
        shi = CharField(max_length=256, null=False)
        mo = CharField(max_length=256, null=False)
        yuguxiaoshoue = CharField(max_length=256, null=False)
        yishou = CharField(max_length=256, null=False)
        shippingfee = CharField(max_length=256, null=False)
        fahuo_time = CharField(max_length=256, null=False)
        sevendays = CharField(max_length=256, null=False)
        onetothree = CharField(max_length=256, null=False)
        protect = CharField(max_length=256, null=False)
        store_name = CharField(max_length=256, null=False)
        product_amount = CharField(max_length=256, null=False)
        product_experience = CharField(max_length=256, null=False)
        seller_service = CharField(max_length=256, null=False)
        shipping_experience = CharField(max_length=256, null=False)
        tuijianyu = CharField(max_length=256, null=False)
        fahuo_city = CharField(max_length=256, null=False)
        quick_refund = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_ss_zbjl_splb(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)

        product_name = CharField(max_length=256, null=False)
        category1 = CharField(max_length=256, null=False)
        category2 = CharField(max_length=256, null=False)
        store_url = CharField(max_length=1024, null=False)
        yuguyongjin = CharField(max_length=256, null=False)
        yongjinratio = CharField(max_length=256, null=False)
        zhibojia = CharField(max_length=256, null=False)
        yuanjian = CharField(max_length=256, null=False)
        quanhoujia = CharField(max_length=256, null=False)
        shangjiatime = CharField(max_length=256, null=False)
        shangjiarenshu = CharField(max_length=256, null=False)
        yuguxiaoliang = CharField(max_length=256, null=False)
        shi = CharField(max_length=256, null=False)
        mo = CharField(max_length=256, null=False)
        yuguxiaoshoue = CharField(max_length=256, null=False)
        yishou = CharField(max_length=256, null=False)
        shippingfee = CharField(max_length=256, null=False)
        fahuo_time = CharField(max_length=256, null=False)
        sevendays = CharField(max_length=256, null=False)
        onetothree = CharField(max_length=256, null=False)
        protect = CharField(max_length=256, null=False)
        store_name = CharField(max_length=256, null=False)
        product_amount = CharField(max_length=256, null=False)
        product_experience = CharField(max_length=256, null=False)
        seller_service = CharField(max_length=256, null=False)
        shipping_experience = CharField(max_length=256, null=False)
        tuijianyu = CharField(max_length=256, null=False)
        fahuo_city = CharField(max_length=256, null=False)
        quick_refund = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    class list_kj_zbjl_splb(BaseModel):
        num_zb = CharField(max_length=256, null=False, index=True)
        id_zb = CharField(max_length=256, null=False, index=True)
        name_zb = CharField(max_length=256, null=False, index=True)
        url_zbjl = CharField(max_length=256, null=False, index=True)
        livestraming_time = CharField(max_length=256, null=False, index=True)

        product_name = CharField(max_length=256, null=False)
        category1 = CharField(max_length=256, null=False)
        category2 = CharField(max_length=256, null=False)
        store_url = CharField(max_length=1024, null=False)
        yuguyongjin = CharField(max_length=256, null=False)
        yongjinratio = CharField(max_length=256, null=False)
        zhibojia = CharField(max_length=256, null=False)
        yuanjian = CharField(max_length=256, null=False)
        quanhoujia = CharField(max_length=256, null=False)
        shangjiatime = CharField(max_length=256, null=False)
        shangjiarenshu = CharField(max_length=256, null=False)
        yuguxiaoliang = CharField(max_length=256, null=False)
        shi = CharField(max_length=256, null=False)
        mo = CharField(max_length=256, null=False)
        yuguxiaoshoue = CharField(max_length=256, null=False)
        yishou = CharField(max_length=256, null=False)
        shippingfee = CharField(max_length=256, null=False)
        fahuo_time = CharField(max_length=256, null=False)
        sevendays = CharField(max_length=256, null=False)
        onetothree = CharField(max_length=256, null=False)
        protect = CharField(max_length=256, null=False)
        store_name = CharField(max_length=256, null=False)
        product_amount = CharField(max_length=256, null=False)
        product_experience = CharField(max_length=256, null=False)
        seller_service = CharField(max_length=256, null=False)
        shipping_experience = CharField(max_length=256, null=False)
        tuijianyu = CharField(max_length=256, null=False)
        fahuo_city = CharField(max_length=256, null=False)
        quick_refund = CharField(max_length=256, null=False)
        time_update = CharField(max_length=256, null=False)

    today_log_dir = '/root/xd_crawler/log/%s' % today_date
    if not os.path.exists(today_log_dir):
        os.mkdir(today_log_dir)

    logging.basicConfig(format='%(message)s', filename=today_log_dir + '/zbjl_splb_%s.log'%type, level=logging.INFO)
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logging.info("--------------------Uncaught Exception--------------------",exc_info=(exc_type, exc_value, exc_traceback))
    sys.excepthook = handle_exception

    today_splb_count = 0

    if current_taks == ' Daily ':
        update_date = lastday_date

    elif current_taks == 'History':

        all_history_data_query_cmd = "list_%s_zbjl_splb.select().where(list_%s_zbjl_splb.time_update.endswith('History')).order_by(list_%s_zbjl_splb.livestraming_time).limit(10)" % (type, type, type)
        today_history_data_query_cmd = "list_%s_zbjl_splb.select().where(list_%s_zbjl_splb.time_update.endswith('History'),list_%s_zbjl_splb.time_update.startswith('%s')).order_by(list_%s_zbjl_splb.livestraming_time).limit(10)" % (type, type, type, today_date, type)

        all_query_result = eval(all_history_data_query_cmd)
        today_query_result = eval(today_history_data_query_cmd)

        if bool(all_query_result):
            # 可能是非首日,也可能是首日、但程序中断存在部分首日历史数据
            if bool(today_query_result):
                # 首日/非首日的第二次运行,即程序异常导致数据中断情景
                latest_history_date = today_query_result[0].livestraming_time.split(' ')[0]
                update_date = latest_history_date
            else:
                # 非首日,日更运行场景
                latest_history_date = all_query_result[0].livestraming_time.split(' ')[0]
                update_date = (datetime.datetime.strptime(latest_history_date, "%Y-%m-%d") + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
        else:
            # 首日第一次运行,数据库中没有任何带标记的历史数据，即也不存在任何今日历史数据
            latest_history_date = lastday_date
            update_date = (datetime.datetime.strptime(latest_history_date, "%Y-%m-%d") + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")

    query_cmd = "list_%s_zbjl.select().where(list_%s_zbjl.livestraming_time.startswith('%s')).order_by(list_%s_zbjl.time_update)" % (type, type, update_date, type)

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
            logging.info(' '.join(['[%s]'%current_taks, type, 'zbjl_splb', one_record.num_zb, one_record.name_zb, webcast_id, one_record.livestraming_time, 'Find local websocket file failed  at', get_current_time()]))
            continue

        with open('/root/xd_crawler/websocket_data/%s.commodity' % webcast_id, 'r') as f:
            commodity_data = json.load(f)

        splb_count = 0
        for product in commodity_data:

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

            # item2_url = product.get('detail_url')
            # session.get(url=item2_url, headers=item2_page_headers)

            product_id = product.get('product_id')
            WAIT_TIME = 0.4

            # staticitem_url = 'https://ec.snssdk.com/product/fxgajaxstaticitem?b_type_new=0&device_id=0&is_outside=1&id={0}&preview=0'.format(product_id)
            staticitem_url = 'https://ec.snssdk.com/product/fxgajaxstaticitem?id={0}'.format(product_id)

            while 1:
                try:
                    time.sleep(WAIT_TIME)
                    # rsp = requests.get(staticitem_url, headers={'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'})
                    # rsp = session.get(url=staticitem_url, headers=pseudo_header)
                    rsp = requests.get(staticitem_url, headers=pseudo_header)
                    data = json.loads(rsp.text).get('data')
                except:
                    logging.info('[*] Get zbjl_splb staticitem_url failed. type:%s, num_zb:%s, url_zbjl:%s at %s' % (type, one_record.num_zb, one_record.url_zbjl, get_current_time()))
                    time.sleep(5)
                else:
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

            # ajaxitem_url = 'https://ec.snssdk.com/product/ajaxitem?b_type_new=0&device_id=0&is_outside=1&id={0}&abParams=0'.format(product_id)
            ajaxitem_url = 'https://ec.snssdk.com/product/ajaxitem?id={0}'.format(product_id)

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
                Table_obj.product_amount = '0'
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

for current_taks in Entry_list:
    # p = Pool(3)
    # for type in input_type:
    #     p.apply_async(run_crawler_task, args=(type, current_taks))
    # p.close()
    # p.join()
    arg = [(i,current_taks) for i in input_type]
    p = Pool(3)
    p.starmap(run_crawler_task,arg)