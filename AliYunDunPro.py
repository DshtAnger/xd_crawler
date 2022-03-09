# coding=utf-8

import logging
import datetime,time
import os,sys
from multiprocessing import Pool

server_type = sys.argv[1]

type_list = {
    'ms':'\n'.join(['ms','ly','kj']),
    'yl':'\n'.join(['yl','cy','ty']),
    'gx':'\n'.join(['gx','ss','jk'])
}

with open('/root/xd_crawler/type','w') as f:
    f.write(type_list.get(server_type))

def get_current_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

today_date = datetime.datetime.now().strftime("%Y-%m-%d")
lastday_date = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
first_crawl_date = (datetime.datetime.now()+datetime.timedelta(days=-121)).strftime("%Y-%m-%d")

today_log_dir = '/root/xd_crawler/log/%s' % today_date
if not os.path.exists(today_log_dir):
    os.mkdir(today_log_dir)

# if int(os.popen('du -l -d 1 /root/xd_crawler/websocket_data/').read().split()[0]) > 1024*1024*3:
#     os.system('rm -rf /root/xd_crawler/websocket_data/')
#     os.system('mkdir /root/xd_crawler/websocket_data/')

if int(os.popen('du -l -d 1 /root/xd_crawler/log/').read().split()[-2]) > 1024*1024:
    os.system('rm -rf /root/xd_crawler/log/*')

# 初始化数据库
db_init = 'python3 /root/xd_crawler/DB.py'

# 登陆账号,得到cookie
login = 'python3 /root/xd_crawler/login.py %s' % server_type

# 首日启动时运行一次
zb_rootdir = 'python3 /root/xd_crawler/zb_rootdir.py'

# 每日更新
zb_xx = 'python3 /root/xd_crawler/zb_xx.py'

# 首日启动时运行一次
zb_qsbx = 'python3 /root/xd_crawler/zb_qsbx.py &'

# 每日更新
zb_zplb = 'python3 /root/xd_crawler/zplb.py'
zb_zplb_pl = 'python3 /root/xd_crawler/zplb_pl.py &'
zb_zplb_qsbx = 'python3 /root/xd_crawler/zplb_qsbx.py &'
zb_zplb_cyzhx = 'python3 /root/xd_crawler/zplb_cyzhx.py &'
zb_zplb_glsp = 'python3 /root/xd_crawler/zplb_glsp.py &'

# 每日更新
zbjl = 'python3 /root/xd_crawler/zbjl.py'
zbjl_sx = 'python3 /root/xd_crawler/zbjl_sx.py &'
zbjl_rc = 'python3 /root/xd_crawler/zbjl_rc.py &'
zbjl_pl = 'python3  /root/xd_crawler/zbjl_pl.py &'
zbjl_splb = 'python3 /root/xd_crawler/zbjl_splb.py &'
zbjl_tlsc = 'python3 /root/xd_crawler/zbjl_tlsc.py &'
zbjl_fd = 'python3 /root/xd_crawler/zbjl_fd.py &'
zbjl_yrsp = 'python3 /root/xd_crawler/zbjl_yrsp.py &'
zbjl_jcfx = 'python3 /root/xd_crawler/zbjl_jcfx.py &'

'------------------------------------------------------------------------------'
try:
    os.system(db_init)
except Exception as e:
    logging.exception(e)
    print('db_init error')

try:
    os.system(login)
except Exception as e:
    logging.exception(e)
    print('login error')

'------------------------------------------------------------------------------'

try:
    os.system(zb_rootdir)
except Exception as e:
    logging.exception(e)
    print('zb_rootdir error')


try:
    os.system(zb_xx)
except Exception as e:
    logging.exception(e)
    print('zb_xx error')


try:
    os.system(zb_qsbx)
except Exception as e:
    logging.exception(e)
    print('zb_qsbx error')

'------------------------------------------------------------------------------'

try:
    os.system(zb_zplb)
except Exception as e:
    logging.exception(e)
    print('zb_zplb error')


try:
    os.system(zb_zplb_pl)
except Exception as e:
    logging.exception(e)
    print('zb_zplb_pl error')

try:
    os.system(zb_zplb_qsbx)
except Exception as e:
    logging.exception(e)
    print('zb_zplb_qsbx error')

try:
    os.system(zb_zplb_cyzhx)
except Exception as e:
    logging.exception(e)
    print('zb_zplb_cyzhx error')

try:
    os.system(zb_zplb_glsp)
except Exception as e:
    logging.exception(e)
    print('zb_zplb_glsp error')

'------------------------------------------------------------------------------'

try:
    os.system(zbjl)
except Exception as e:
    logging.exception(e)
    print('zbjl error')


try:
    os.system(zbjl_sx)
except Exception as e:
    logging.exception(e)
    print('zbjl_sx error')


try:
    os.system(zbjl_rc)
except Exception as e:
    logging.exception(e)
    print('zbjl_rc error')


try:
    os.system(zbjl_pl)
except Exception as e:
    logging.exception(e)
    print('zbjl_pl error')


try:
    os.system(zbjl_splb)
except Exception as e:
    logging.exception(e)
    print('zbjl_splb error')


try:
    os.system(zbjl_tlsc)
except Exception as e:
    logging.exception(e)
    print('zbjl_tlsc error')


try:
    os.system(zbjl_fd)
except Exception as e:
    logging.exception(e)
    print('zbjl_fd error')

# 8点启动无法获取到数据,可能是xd更新数据时间的问题,该任务单独用crontab定为12点启动
# try:
#     os.system(zbjl_yrsp)
# except Exception as e:
#     logging.exception(e)
#     print('zbjl_yrsp error')


try:
    os.system(zbjl_jcfx)
except Exception as e:
    logging.exception(e)
    print('zbjl_jcfx error')

'------------------------------------------------------------------------------'

#
#
# def run_crawler_task(task_cmd,task_name):
#     print('[+] Run  Task: %s Pid:%d At %s' % (task_name, os.getpid(), get_current_time()))
#     try:
#         os.system(task_cmd)
#     except Exception as e:
#         logging.exception(e)
#     print('[+] Done Task: %s Pid:%d At %s' % (task_name, os.getpid(), get_current_time()))
#     print('-'*100)
#
# print( 'Parent Process %s Started At %s' % (os.getpid(), get_current_time()) )
# print('-'*100)
#
# '------------------------------------------------------------------------------'
#
# if today_date == FIRST_RUN_DATE:
#     zb_rootdir_task = Process(target=run_crawler_task, args=(zb_rootdir,'zb_rootdir'))
#     zb_rootdir_task.start()
#     zb_rootdir_task.join()
#
# zb_xx_task = Process(target=run_crawler_task, args=(zb_xx, 'zb_xx'))
# zb_xx_task.start()
# zb_xx_task.join()
#
# if today_date == FIRST_RUN_DATE:
#     zb_qsbx_task = Process(target=run_crawler_task, args=(zb_qsbx,'zb_qsbx'))
#     zb_qsbx_task.start()
#     zb_qsbx_task.join()
#
# '------------------------------------------------------------------------------'
#
# zb_zplb_task = Process(target=run_crawler_task, args=(zb_zplb, 'zb_zplb'))
# zb_zplb_task.start()
# zb_zplb_task.join()
#
# zb_zplb_tables_pool = Pool()
# zb_zplb_tables_pool.apply_async(run_crawler_task, args=(zb_zplb_pl,'zb_zplb_pl'))
# zb_zplb_tables_pool.apply_async(run_crawler_task, args=(zb_zplb_qsbx, 'zb_zplb_qsbx'))
# zb_zplb_tables_pool.apply_async(run_crawler_task, args=(zb_zplb_cyzhx, 'zb_zplb_cyzhx'))
# zb_zplb_tables_pool.apply_async(run_crawler_task, args=(zb_zplb_glsp, 'zb_zplb_glsp'))
# zb_zplb_tables_pool.close()
# zb_zplb_tables_pool.join()
#
# '------------------------------------------------------------------------------'
#
# zbjl_task = Process(target=run_crawler_task, args=(zbjl, 'zbjl'))
# zbjl_task.start()
# zbjl_task.join()
#
# zbjl_tables_pool = Pool()
# zbjl_tables_pool.apply_async(run_crawler_task, args=(zbjl_sx,'zbjl_sx'))
# zbjl_tables_pool.apply_async(run_crawler_task, args=(zbjl_splb, 'zbjl_splb'))
# zbjl_tables_pool.apply_async(run_crawler_task, args=(zbjl_pl, 'zbjl_pl'))
# zbjl_tables_pool.apply_async(run_crawler_task, args=(zbjl_zzgm, 'zbjl_zzgm'))
# zbjl_tables_pool.apply_async(run_crawler_task, args=(zbjl_ll_rc, 'zbjl_ll_rc'))
# zbjl_tables_pool.close()
# zbjl_tables_pool.join()