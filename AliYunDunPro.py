# coding=utf-8

import os

zb_xx = 'python3 -u /root/xd_crawler/zb_xx.py >> /root/xd_crawler/log/zb_xx.log 2>&1 &'

#只运行一次,用crontab单独启动
#zb_qsbx = 'python3 -u /root/xd_crawler/zb_qsbx.py >> /root/xd_crawler/log/zb_qsbx.log 2>&1 &'

zb_zplb = 'python3 -u /root/xd_crawler/zb_zplb.py >> /root/xd_crawler/log/zb_zplb.log 2>&1'
zb_zplb_pl = 'python3 -u /root/xd_crawler/zb_zplb_pl.py >> /root/xd_crawler/log/zb_zplb_pl.log 2>&1 &'
zb_zplb_qsbx = 'python3 -u /root/xd_crawler/zb_zplb_qsbx.py >> /root/xd_crawler/log/zb_zplb_qsbx.log 2>&1 &'
zb_zplb_cyzhx = 'python3 -u /root/xd_crawler/zb_zplb_cyzhx.py >> /root/xd_crawler/log/zb_zplb_cyzhx.log 2>&1 &'
zb_zplb_glsp = 'python3 -u /root/xd_crawler/zb_zplb_glsp.py >> /root/xd_crawler/log/zb_zplb_glsp.log 2>&1 &'

zbjl = 'python3 -u /root/xd_crawler/zbjl.py >> /root/xd_crawler/log/zbjl.log 2>&1'
zbjl_sx = 'python3 -u /root/xd_crawler/zbjl_sx.py >> /root/xd_crawler/log/zbjl_sx.log 2>&1 &'
zbjl_splb = 'python3 -u /root/xd_crawler/zbjl_splb.py  >> /root/xd_crawler/log/zbjl_splb.log 2>&1 &'

zbjl_pl = 'python3  -u /root/xd_crawler/zbjl_pl.py >> /root/xd_crawler/log/zbjl_pl.log 2>&1 &'
zbjl_zzgm = 'python3 -u /root/xd_crawler/zbjl_zzgm.py >> /root/xd_crawler/log/zbjl_zzgm.log 2>&1 &'
zbjl_ll_rc = 'python3 -u /root/xd_crawler/zbjl_ll-rc-yl-fst-dz-tlsc.py >> /root/xd_crawler/log/zbjl_ll_rc.log 2>&1 &'

'------------------------------------------------------------------------------'

try:
    os.system(zb_xx)
except:
    print('zb_xx error')

'------------------------------------------------------------------------------'

try:
    os.system(zb_zplb)
except:
    print('zb_zplb error')


try:
    os.system(zb_zplb_pl)
except:
    print('zb_zplb_pl error')

try:
    os.system(zb_zplb_qsbx)
except:
    print('zb_zplb_qsbx error')

try:
    os.system(zb_zplb_cyzhx)
except:
    print('zb_zplb_cyzhx error')

try:
    os.system(zb_zplb_glsp)
except:
    print('zb_zplb_glsp error')

'------------------------------------------------------------------------------'

try:
    os.system(zbjl)
except:
    print('zbjl error')


try:
    os.system(zbjl_sx)
except:
    print('zbjl_sx error')

try:
    os.system(zbjl_splb)
except:
    print('zbjl_splb error')


try:
    os.system(zbjl_pl)
except:
    print('zbjl_pl error')

try:
    os.system(zbjl_zzgm)
except:
    print('zbjl_zzgm error')

try:
    os.system(zbjl_ll_rc)
except:
    print('zbjl_ll_rc error')

'------------------------------------------------------------------------------'



