# coding=utf-8

import logging
import datetime,time
import os
from multiprocessing import Pool

def get_current_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

today_date = datetime.datetime.now().strftime("%Y-%m-%d")
lastday_date = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
first_crawl_date = (datetime.datetime.now()+datetime.timedelta(days=-121)).strftime("%Y-%m-%d")
FIRST_RUN_DATE = today_date

today_log_dir = '/root/xd_crawler/log/%s' % today_date
if not os.path.exists(today_log_dir):
    os.mkdir(today_log_dir)

# 首日启动时运行一次
zb_rootdir = 'python3 -u /root/xd_crawler/zb_rootdir.py >> /root/xd_crawler/log/zb_rootdir.log 2>&1'

# 每日更新
zb_xx = 'python3 -u /root/xd_crawler/zb_xx.py >> /root/xd_crawler/log/zb_xx.log 2>&1'

# 首日启动时运行一次
zb_qsbx = 'python3 -u /root/xd_crawler/zb_qsbx.py >> /root/xd_crawler/log/zb_qsbx.log 2>&1 &'

# 每日更新
zb_zplb = 'python3 -u /root/xd_crawler/zb_zplb.py >> /root/xd_crawler/log/zb_zplb.log 2>&1'
zb_zplb_pl = 'python3 -u /root/xd_crawler/zb_zplb_pl.py >> /root/xd_crawler/log/zb_zplb_pl.log 2>&1 &'
zb_zplb_qsbx = 'python3 -u /root/xd_crawler/zb_zplb_qsbx.py >> /root/xd_crawler/log/zb_zplb_qsbx.log 2>&1 &'
zb_zplb_cyzhx = 'python3 -u /root/xd_crawler/zb_zplb_cyzhx.py >> /root/xd_crawler/log/zb_zplb_cyzhx.log 2>&1 &'
zb_zplb_glsp = 'python3 -u /root/xd_crawler/zb_zplb_glsp.py >> /root/xd_crawler/log/zb_zplb_glsp.log 2>&1 &'

# 每日更新
zbjl = 'python3 -u /root/xd_crawler/zbjl.py >> /root/xd_crawler/log/zbjl.log 2>&1'
zbjl_sx = 'python3 -u /root/xd_crawler/zbjl_sx.py >> /root/xd_crawler/log/zbjl_sx.log 2>&1 &'
zbjl_splb = 'python3 -u /root/xd_crawler/zbjl_splb.py  >> /root/xd_crawler/log/zbjl_splb.log 2>&1 &'
zbjl_pl = 'python3  -u /root/xd_crawler/zbjl_pl.py >> /root/xd_crawler/log/zbjl_pl.log 2>&1 &'
zbjl_zzgm = 'python3 -u /root/xd_crawler/zbjl_zzgm.py >> /root/xd_crawler/log/zbjl_zzgm.log 2>&1 &'
zbjl_ll_rc = 'python3 -u /root/xd_crawler/zbjl_ll-rc-yl-fst-dz-tlsc.py >> /root/xd_crawler/log/zbjl_ll_rc.log 2>&1 &'

'------------------------------------------------------------------------------'

if today_date == FIRST_RUN_DATE:
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


if today_date == FIRST_RUN_DATE:
    try:
        os.system(zb_qsbx)
    except Exception as e:
        logging.exception(e)
        print('zb_qsbx error')

'------------------------------------------------------------------------------'

# try:
#     os.system(zb_zplb)
# except Exception as e:
#     logging.exception(e)
#     print('zb_zplb error')
#
#
# try:
#     os.system(zb_zplb_pl)
# except Exception as e:
#     logging.exception(e)
#     print('zb_zplb_pl error')
#
# try:
#     os.system(zb_zplb_qsbx)
# except Exception as e:
#     logging.exception(e)
#     print('zb_zplb_qsbx error')
#
# try:
#     os.system(zb_zplb_cyzhx)
# except Exception as e:
#     logging.exception(e)
#     print('zb_zplb_cyzhx error')
#
# try:
#     os.system(zb_zplb_glsp)
# except Exception as e:
#     logging.exception(e)
#     print('zb_zplb_glsp error')

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
    os.system(zbjl_splb)
except Exception as e:
    logging.exception(e)
    print('zbjl_splb error')


try:
    os.system(zbjl_pl)
except Exception as e:
    logging.exception(e)
    print('zbjl_pl error')

try:
    os.system(zbjl_zzgm)
except Exception as e:
    logging.exception(e)
    print('zbjl_zzgm error')

try:
    os.system(zbjl_ll_rc)
except Exception as e:
    logging.exception(e)
    print('zbjl_ll_rc error')

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