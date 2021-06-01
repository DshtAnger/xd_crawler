# coding=utf-8

import requests
import websocket
import json
import datetime
import time
from DB import *

today_date = datetime.datetime.now().strftime("%Y-%m-%d")
first_crawl_date = (datetime.datetime.now()+datetime.timedelta(days=-30*4)).strftime("%Y-%m-%d")
update_date = (datetime.datetime.now()+datetime.timedelta(days=-2)).strftime("%Y-%m-%d")
#是否为第一次抓取判断依据：启动时先查询数据库有没有4个月前后几天发布的数据，若有则进行两天更新，若没有则是第一次抓取。

with open('./cookie','r') as f:
    cookie = f.read().strip()
with open('./token','r') as f:
    token = f.read().strip()

search_type = {
    '娱乐':'yl','才艺':'cy','萌宠':'mc',
    '搞笑':'gx','游戏':'yx','家居':'jj',
    '美食':'ms','旅游':'ly','健康':'jk',
    '汽车':'qc','时尚':'ss','社会':'sh'
}
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


for type in list(search_type.values()):

    if type != 'ms':
        continue

    for one_record in eval('list_' + type + '.select()'):


        uid = one_record.url_zb.split('/')[-1]
        webcastList_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/webcastList'
        post_data = {
            "date_type": "",
            "size": 100,
            "sort": "",
            "start": 1,
            "has_commerce_goods": "",
            "uid": uid
        }
        while 1:
            try:
                rsp = requests.post(webcastList_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data')
                count = data.get('count')
                end_page = int(count / 100) + 1 if count % 100 != 0 else int(count / 100)
            except:
                print(
                    '[*] Get zbjl webcastList_url count faile. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb,time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                time.sleep(5)
            else:
                break

        for page in range(1,end_page+1):

            while 1:
                try:
                    post_data.update({'start': page})
                    rsp = requests.post(webcastList_url, headers=headers, data=json.dumps(post_data))
                    data_list = json.loads(rsp.text).get('data').get('list')
                except:
                    print(
                        '[*] Get zbjl webcastList_url data faile. type:%s, num_zb:%s, url_zb:%s at %s' % (type, one_record.num_zb, one_record.url_zb,time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                    time.sleep(5)
                else:
                    break

            for item in data_list:

                webcast_id = item.get('id')

                Table_obj = eval('list_' + type + '_zbjl' + '.create()')
                Table_obj.num_zb = one_record.num_zb
                Table_obj.id_zb = one_record.id_zb
                Table_obj.name_zb = one_record.name_zb
                Table_obj.url_zbjl = 'https://xd.newrank.cn/d/broadcast/%s' % webcast_id

                websocket_url = 'wss://xd.newrank.cn/xdnphb/nr/cloud/douyin/websocket'
                ws_data = {
                    "type": "webcast",
                    "data": {"room_id": webcast_id}
                }
                ws = websocket.WebSocket()
                ws.connect(websocket_url, cookie=cookie,origin="https://xd.newrank.cn", host="xd.newrank.cn")
                ws.send(json.dumps(ws_data))

                tipRank_data = json.loads(ws.recv()).get('data')
                commodity_data = json.loads(ws.recv()).get('data')
                detail_data = json.loads(ws.recv()).get('data')

                '------------------------------------------------------------------------------'
                livestraming_time = detail_data.get('create_time')

                Table_obj.livestraming_time = detail_data.get('create_time')
                Table_obj.duration = detail_data.get('duration')

                Table_obj.yinlangshouru = detail_data.get('stats_fan_ticket_money')
                Table_obj.benchangyinlang = detail_data.get('stats_fan_ticket')
                Table_obj.songli = detail_data.get('stats_gift_uv_count')
                Table_obj.zaixianfengzhi = detail_data.get('max_user_count')
                Table_obj.leijiguankan = detail_data.get('stats_total_user')
                Table_obj.zongdianzan = detail_data.get('like_count')
                Table_obj.danchangzhangfen = detail_data.get('add_fans_count')
                Table_obj.zhuanfenlv = detail_data.get('turn_rate')
                Table_obj.pingjunzaixian = detail_data.get('avg_user_count')
                Table_obj.pingjunzhiliu = detail_data.get('user_average_duration')

                Table_obj.yuguxiaoshoue = str(detail_data.get('total_sales_money'))
                Table_obj.yuguxiaoshouliang = detail_data.get('total_sales')
                Table_obj.shangjiashangpin = detail_data.get('promotion_count')

                Table_obj.zuigaodanjia = detail_data.get('max_price')
                Table_obj.zuigaoxiaoliang = detail_data.get('max_sales')
                Table_obj.zuigaoxiaoshoue = detail_data.get('max_sales_money')
                Table_obj.kedanjia = str(detail_data.get('customer_price'))
                Table_obj.renjungoumaijiazhi = detail_data.get('per_capita')
                Table_obj.xiaoshouzhuanhualv = detail_data.get('conversion_rate')
                Table_obj.time_update = today_date

                Table_obj.save()

                print('[+]', type, 'zbjl', one_record.num_zb, one_record.name_zb, aweme_id, 'Done at',time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

                '------------------------------------------------------------------------------'

                for product in commodity_data:

                    Table_obj = eval('list_' + type + '_zbjl_splb' + '.create()')
                    Table_obj.num_zb = one_record.num_zb
                    Table_obj.id_zb = one_record.id_zb
                    Table_obj.name_zb = one_record.name_zb
                    Table_obj.url_zbjl = 'https://xd.newrank.cn/d/broadcast/%s' % webcast_id

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
                            rsp = requests.get(staticitem_url, headers={'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'})
                            if not json.loads(rsp.text).get('data'):
                                raise Exception
                        except:
                            print(
                                '[*] Get zbjl_splb staticitem_url faile. type:%s, num_zb:%s, url_zb:%s at %s' % (
                                type, one_record.num_zb, one_record.url_zb,
                                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                            time.sleep(5)
                        else:
                            data = json.loads(rsp.text).get('data')
                            break

                    Table_obj.yishou = data.get('sell_num')
                    Table_obj.fahuo_time = data.get('detail_delay_desc')

                    if data.get('freight'):
                        Table_obj.fahuo_city = data.get('freight').get('product_province_name') if data.get('freight') else '--'
                        Table_obj.shippingfee = '不确定'
                    else:
                        Table_obj.fahuo_city = '--'
                        Table_obj.shippingfee = '包邮'

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

                    pingjia_dic = {'1':'高','2':'中','3':'低'}
                    Table_obj.store_name = data.get('shop_name')
                    Table_obj.product_experience = str(data.get('credit_score').get('product')) + pingjia_dic[str(data.get('credit_score').get('gap_product'))] if data.get('credit_score') else '--'
                    Table_obj.seller_service = str(data.get('credit_score').get('shop')) + pingjia_dic[str(data.get('credit_score').get('gap_shop'))] if data.get('credit_score') else '--'
                    Table_obj.shipping_experience = str(data.get('credit_score').get('logistics')) + pingjia_dic[str(data.get('credit_score').get('gap_logistics'))] if data.get('credit_score') else '--'
                    Table_obj.tuijianyu = data.get('recommend_remark')


                    while 1:
                        try:
                            ajaxitem_url = 'https://ec.snssdk.com/product/ajaxitem?b_type_new=0&device_id=0&is_outside=1&id={0}&abParams=0'.format(product_id)
                            rsp = requests.get(ajaxitem_url, headers={'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'})
                        except:
                            print(
                                '[*] Get zbjl_splb ajaxitem_url faile. type:%s, num_zb:%s, url_zb:%s at %s' % (
                                type, one_record.num_zb, one_record.url_zb,
                                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                            time.sleep(5)
                        else:
                            data = json.loads(rsp.text)
                            break

                    # data有可能是[],也可能是{"st":10024,"msg":"商品已下架","data":null}
                    if data == [] or data.get('data') == None:
                        Table_obj.product_amount = '0'
                    else:
                        Table_obj.product_amount = data.get('data').get('shop_product_count') if data.get('data') else '--'

                    Table_obj.time_update = today_date
                    #print(livestraming_time,product_name,commodity_data.index(product))

                    Table_obj.save()

                '------------------------------------------------------------------------------'
                continue

                shipintuijian = detail_data.get('stats_user_count_composition').get('video_detail')
                guanzhu = detail_data.get('stats_user_count_composition').get('my_follow')
                zhiboguangchang = detail_data.get('stats_user_count_composition').get('other')

                male, female = ['--'] * 2
                for gender in detail_data.get('watch_user_gender'):
                    if gender.get('key') == "1":
                        male = gender.get('rate')
                    if gender.get('key') == "2":
                        female = gender.get('rate')

                city_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/webDetail/city'
                post_data = {
                    "room_id": webcast_id
                }
                rsp = requests.post(city_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data')


                heilongjiang = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='黑龙江',data)])
                jilin = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='吉林',data)])
                liaoning = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='辽宁',data)])
                neimeng = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='蒙古',data)])
                hebei = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='河北',data)])
                beijing = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='北京',data)])
                tianjin = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='天津',data)])
                shandong = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='山东',data)])
                shanxi = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='山西',data)])
                henan = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='河南',data)])
                anhui = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='安徽',data)])
                jiangsu = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='江苏',data)])
                shanghai = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='上海',data)])
                zhejing = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='浙江',data)])
                jiangxi = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='江西',data)])
                fujian = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='福建',data)])
                taiwan = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='台湾',data)])
                guangdong = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='广东',data)])
                guangxi = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='广西',data)])
                hainan = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='海南',data)])
                yunnan = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='云南',data)])
                guizhou = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='贵州',data)])
                hunan = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='湖南',data)])
                chongqing = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='重庆',data)])
                hubei = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='湖北',data)])
                shaanxi = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='陕西',data)])
                ningxia = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='宁夏',data)])
                gansu = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='甘肃',data)])
                sichuan = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='四川',data)])
                qinghai = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='青海',data)])
                xinjiang = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='新疆',data)])
                xizhang = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='西藏',data)])
                xianggang = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='香港',data)])
                aomen = ''.join([i.get('rate') for i in filter(lambda x: x.get('key')=='澳门',data)])


                '------------------------------------------------------------------------------'

                comment_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/webcastMessageList'
                post_data = {
                    "room_id": webcast_id,
                    "size": 100,
                    "start": 1,
                }
                rsp = requests.post(comment_url, headers=headers, data=json.dumps(post_data))
                data = json.loads(rsp.text).get('data')
                count = data.get('count')
                end_page = int(count / 100) + 1 if count % 100 != 0 else int(count / 100)

                for page in range(1, end_page+1):
                    post_data.update({'start': page})
                    rsp = requests.post(comment_url, headers=headers, data=json.dumps(post_data))
                    data = json.loads(rsp.text).get('data')
                    data_list = data.get('list')

                    for item in data_list:
                        review_time = item.get('create_time')
                        author = item.get('user').get('nickname')
                        content = item.get('content')
                        time_update = today_date
                        #print(review_time,author,content)

                '------------------------------------------------------------------------------'

                trend_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/webDetail/trend'
                post_data = {
                    "room_id": webcast_id
                }
                rsp = requests.post(trend_url, headers=headers, data=json.dumps(post_data))
                data_list = json.loads(rsp.text).get('data').get('webcastTrendList')

                for item in data_list:
                    crawl_time = item.get('crawl_date')

                    renshu = item.get('user_count')

                    leijirenci = item.get('stats_total_user')
                    guanzhu = item.get('stats_user_composition_from_my_follow')
                    shipintuijian = item.get('stats_user_composition_from_video_detail')
                    zhiboguangchang = item.get('stats_user_composition_from_other')

                    yinlang = item.get('stats_fan_ticket')

                    fans_zb = item.get('club_info_total_fans_count')

                    support_zb = item.get('like_count')


                buy_url = 'https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/webcast/webDetail/buy'
                post_data = {
                    "room_id": webcast_id
                }
                rsp = requests.post(buy_url, headers=headers, data=json.dumps(post_data))
                data_list = json.loads(rsp.text).get('data').get('webcastBuyDTOS')

                for item in data_list[::-1]:
                    purchase_time = item.get('create_time')
                    purchase = item.get('purchase_cnt')
                    #print(purchase_time,'---',purchase)
