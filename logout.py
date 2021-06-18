# coding=utf-8

import requests,json
import os

with open('/root/xd_crawler/cookie','r') as f:
    cookie = f.read().strip()

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'zh-CN,zh;q=0.9',
    'content-type': 'application/json;charset=UTF-8',
    'cookie': cookie,
    'origin': 'https://xd.newrank.cn',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36',
}

logout_url = 'https://xd.newrank.cn/nr/user/login/loginOut'
requests.post(logout_url,headers=headers, data=json.dumps({}))

os.remove('/root/xd_crawler/cookie')