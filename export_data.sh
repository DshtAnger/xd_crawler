#!/bin/bash

echo "[+] Export database xd all data start At `date '+%Y-%m-%d %H:%M:%S'`"

mysqldump -uroot -pWanghongpeng1 xd | zip > xd_`date '+%Y%m%d'`.zip 2>/dev/null

if [ `hostname` == "ms" ]
then
  mysqldump -uroot -pWanghongpeng1 xd list_ms > xd_list_ms.sql 2>/dev/null
  mysqldump -uroot -pWanghongpeng1 xd list_ss > xd_list_ss.sql 2>/dev/null
  mysqldump -uroot -pWanghongpeng1 xd list_kj > xd_list_kj.sql 2>/dev/null
  mysql -uroot -pWanghongpeng1 <<EOF
  drop database xd;
  create database xd;
  use xd;
  source xd_list_ms.sql;
  source xd_list_ss.sql;
  source xd_list_kj.sql;
EOF
  rm xd_list_ms.sql xd_list_ss.sql xd_list_kj.sql
elif [ `hostname` == "yl" ]
then
  mysqldump -uroot -pWanghongpeng1 xd list_yl > xd_list_yl.sql 2>/dev/null
  mysqldump -uroot -pWanghongpeng1 xd list_cy > xd_list_cy.sql 2>/dev/null
  mysqldump -uroot -pWanghongpeng1 xd list_ty > xd_list_ty.sql 2>/dev/null
  mysql -uroot -pWanghongpeng1 <<EOF
  drop database xd;
  create database xd;
  use xd;
  source xd_list_yl.sql;
  source xd_list_cy.sql;
  source xd_list_ty.sql;
EOF
  rm xd_list_yl.sql xd_list_cy.sql xd_list_ty.sql
elif [ `hostname` == "gx" ]
then
  mysqldump -uroot -pWanghongpeng1 xd list_gx > xd_list_gx.sql 2>/dev/null
  mysqldump -uroot -pWanghongpeng1 xd list_ly > xd_list_ly.sql 2>/dev/null
  mysqldump -uroot -pWanghongpeng1 xd list_jk > xd_list_jk.sql 2>/dev/null
  mysql -uroot -pWanghongpeng1 <<EOF
  drop database xd;
  create database xd;
  use xd;
  source xd_list_gx.sql;
  source xd_list_ly.sql;
  source xd_list_jk.sql;
EOF
  rm xd_list_gx.sql xd_list_ly.sql xd_list_jk.sql
fi

echo "[+] Export database xd all data  done At `date '+%Y-%m-%d %H:%M:%S'`"

