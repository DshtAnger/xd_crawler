# coding=utf-8



account_list = {
    'ms':[('wanghongpeng','Wanghongpeng1'),('changzhao','duqianzhou@1907')],
    'yl':[('fsx456','123456'),('pangruhuan','duqianzhou@1907')],
    'gx':[('liuyuanyuan','duqianzhou@1907'),('zhudingye','duqianzhou@1907')]
}

def get_current_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

today_date = datetime.datetime.now().strftime("%Y-%m-%d")
today_log_dir = '/root/xd_crawler/log/%s' % today_date
if not os.path.exists(today_log_dir):
    os.mkdir(today_log_dir)
logging.basicConfig(format='%(message)s',filename=today_log_dir + '/login.log', level=logging.INFO)
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.info("--------------------Uncaught Exception--------------------",exc_info=(exc_type, exc_value, exc_traceback))
sys.excepthook = handle_exception



async def login(USERNAME, PASSWORD):

from pyppeteer import launch
from pyppeteer.network_manager import Response
import asyncio
import os, sys
import logging
import datetime, time

async def get_content(response: Response):
    """
    # 注意这里不须要设置 page.setRequestInterception(True)
    page.on("response", get_content)
    :param response:
    :return:
    """

    if response.url == "https://gw.newrank.cn/api/xd/xdnphb/nr/cloud/douyin/login":
        status = await response.status()
        print(status)
    else:
        print('aaa')

USERNAME = 'wanghongpeng'
PASSWORD = 'Wanghongpeng1'
browser = await launch(headless=True,options={'args': ['--no-sandbox']},ignoreDefaultArgs=['--enable-automation']) # 关闭无头浏览器
page = await browser.newPage()
await page.setJavaScriptEnabled(enabled=True)
await page.setUserAgent('5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36')
await page.evaluateOnNewDocument('''() =>{ Object.defineProperties(navigator,{ webdriver:{ get: () => false } }) }''')
await page.evaluateOnNewDocument('''() =>{ window.navigator.chrome = { runtime: {},  }; }''')
await page.evaluateOnNewDocument('''() =>{ Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] }); }''')
await page.evaluateOnNewDocument('''() =>{ Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5,6], }); }''')
await page.setRequestInterception(True)
page.on("response", get_content)
await page.goto('https://xd.newrank.cn/home/index',options={'timeout': 10000})

login_button = await page.xpath('//*[@id="nr-pro-header"]/div[2]/button/span')
await login_button[0].click()
time.sleep(3)

other_login = await page.xpath('/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div[3]/span[1]')
await other_login[0].click()
time.sleep(3)

username = await page.xpath('/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div[2]/div[1]/input')
await username[0].type(USERNAME)
time.sleep(3)

password = await page.xpath('/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div[2]/div[2]/span/input')
await password[0].type(PASSWORD)
time.sleep(3)

keep_login = await page.xpath('/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div[2]/div[3]/label/span[1]/input')
await keep_login[0].click()
time.sleep(3)

login_button = await page.xpath('/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div[2]/div[4]/button')
await login_button[0].click()
time.sleep(3)

# https://cloud.tencent.com/developer/article/1763510
frame = page.frames
await frame[0].hover('#nc_1_n1z')
await page.mouse.down()
await page.mouse.move(2000, 0)
time.sleep(3)

    cookies = await page.cookies()
    cookie = ''
    for one in cookies:
        if one.get('name') == 'token':
            cookie = 'token=' + one.get('value')

    if cookie != '':

        logging.info('[+] Get Today %s cookie: %s at %s'%(USERNAME, cookie, get_current_time()))

        with open('/root/xd_crawler/cookie','w') as f:
            f.write(cookie)

    else:
        logging.info('[+] Get Today %s cookie Error at %s' % (USERNAME, get_current_time()))

    await browser.close()

    return cookie

if __name__ == '__main__':

    server_type = sys.argv[1]
    main_account, backup_account = account_list.get(server_type)

    main_result = asyncio.get_event_loop().run_until_complete(login(main_account[0],main_account[1]))

    if main_result == '':
        time.sleep(3)
        backup_result = asyncio.get_event_loop().run_until_complete(login(backup_account[0], backup_account[1]))

# await page.hover('#nr-pro-header > div._Wd5iasy8 > div._3WBnyna6 > span > span > span > img')
# time.sleep(3)
# logout_button = await page.xpath('//*[@id="nr-pro-header"]/div[3]/div/div/div/div[1]/div[2]/span')
# await logout_button[0].click()
# time.sleep(3)


# def random_linspace(num, length):
#
#     start, end = 0.5 * (length / num), 1.5 * (length / num)
#     # 借助三方库生成一个标准的等差数列，主要是得出标准等差 space
#     origin_list = numpy.linspace(start, end, num)
#     space = origin_list[2] - origin_list[1]
#     # 在标准等差的基础上，设置上下浮动的大小，（上下浮动10%）
#     min_random, max_random = -(space / 10), space / 10
#     result = []
#     # 等差数列的初始值不变，就是我们设置的start
#     value = start
#     # 将等差数列添加到 list
#     result.append(value)
#     # 初始值已经添加，循环的次数 减一
#     for i in range(num - 1):
#         # 浮动的等差值 space
#         random_space = space + random.uniform(min_random, max_random)
#         value += random_space
#         result.append(value)
#     return result
# start = 0
# end = 0
# for a in random_linspace(4,800):
#     a = int(a)
#     print(f'滑动{a}')
#     end +=a
#     print(end, start)
#     await  page.mouse.move(end, start)
#     start=end


# login_or_reg = driver.find_element_by_xpath('//*[@id="nr-pro-header"]/div[2]/button/span')
#    ...: login_or_reg.click()
#    ...: time.sleep(1)
#    ...:
#    ...: other_login = driver.find_element_by_xpath('/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div[3]/span[1]')
#    ...: other_login.click()
#    ...: time.sleep(1)
#    ...:
#    ...: username = driver.find_element_by_xpath('/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div[2]/div[1]/input')
#    ...: username.send_keys('wanghongpeng')
#    ...: time.sleep(1)
#    ...:
#    ...: password = driver.find_element_by_xpath('/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div[2]/div[2]/span/input')
#    ...: password.send_keys('123')
#    ...: time.sleep(1)
#    ...:
#    ...: login_button = driver.find_element_by_xpath('/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div[2]/div[4]/button')
#    ...: login_button.click()
#    ...: time.sleep(1)
#    ...:
#    ...: huakuai_button = driver.find_element_by_xpath('//*[@id="nc_1_n1z"]')
#    ...: move_to_gap(huakuai_button,get_track(380))