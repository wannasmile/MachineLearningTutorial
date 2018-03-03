#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
搜索日志

Created by C.L.Wang
"""
import os
import sys

p = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if p not in sys.path:
    sys.path.append(p)

from project_utils import str_to_datetime
from rcm_doctors.search_log_analysis.models import SearchDoctorsObject

from django.db import ConnectionHandler

from configs import settings

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "configs.settings")


def create_sl_table():
    connections = ConnectionHandler(settings.DATABASES)
    cursor = connections['default'].cursor()

    """
    data_type # 推荐组的编号: 0(默认), 1(低价), 2(高价)
    pos_of_list # 位置信息: 1, 2, 3, o
    f_clinic_no # 一级科室
    s_clinic_no # 二级科室
    data_source # 数据来源: cc(click_clinic), se(search), fi(filter), sort(so)
    """
    sql = 'CREATE TABLE if NOT EXISTS search_doctors_log(' \
          'id INT UNSIGNED AUTO_INCREMENT,' \
          'data_type VARCHAR(100) NOT NULL,' \
          'pos_of_list VARCHAR(100) NOT NULL,' \
          'f_clinic_no VARCHAR(100) NOT NULL,' \
          's_clinic_no VARCHAR(100) NOT NULL,' \
          'data_source VARCHAR(100) NOT NULL,' \
          'search_pv INTEGER NOT NULL,' \
          'search_uv INTEGER NOT NULL,' \
          'search_sum INTEGER NOT NULL,' \
          'click_pv INTEGER NOT NULL,' \
          'click_uv INTEGER NOT NULL,' \
          'click_sum INTEGER NOT NULL,' \
          'purchase_pv INTEGER NOT NULL,' \
          'purchase_uv INTEGER NOT NULL,' \
          'purchase_sum INTEGER NOT NULL,' \
          'record_time DATE, ' \
          'last_modified DATETIME, ' \
          'PRIMARY KEY (id))ENGINE=InnoDB DEFAULT CHARSET=utf8;'

    cursor.execute(sql)
    print "创建表search_doctors_log完成!"


def save_db_row(sdo):
    if not isinstance(sdo, SearchDoctorsObject):
        return
    connections = ConnectionHandler(settings.DATABASES)
    cursor = connections['default'].cursor()
    sql = 'INSERT INTO search_doctors_log (' \
          'data_type, pos_of_list, f_clinic_no, s_clinic_no, data_source, ' \
          'search_pv, search_uv, search_sum, ' \
          'click_pv, click_uv, click_sum, ' \
          'purchase_pv, purchase_uv, purchase_sum, ' \
          'record_time, last_modified)' \
          ' VALUES (' \
          '"%s", "%s", "%s", "%s", "%s", ' \
          '%s, %s, %s, ' \
          '%s, %s, %s, ' \
          '%s, %s, %s, ' \
          '"%s", "%s")' % \
          (sdo.data_type, sdo.pos_of_list, sdo.f_clinic_no, sdo.s_clinic_no, sdo.data_source,
           sdo.search_pv, sdo.search_uv, sdo.search_sum,
           sdo.click_pv, sdo.click_uv, sdo.click_sum,
           sdo.purchase_pv, sdo.purchase_uv, sdo.purchase_sum,
           sdo.record_time, sdo.last_modified)
    cursor.execute(sql)
    print "写入search_doctors_log一行!"


def delete_db_row(record_time):
    """
    根据记录时间，删除行数据
    :param record_time: 记录时间，例如：2017-02-25
    :return: Log提示
    """
    if not record_time:
        return
    connections = ConnectionHandler(settings.DATABASES)
    cursor = connections['default'].cursor()
    sql = 'DELETE FROM search_doctors_log WHERE record_time="%s"' % record_time
    cursor.execute(sql)
    print "删除数据成功: %s" % record_time


def test_of_add_one_row():
    sdo = SearchDoctorsObject()
    sdo.data_type = 'default'
    sdo.pos_of_list = '1'
    sdo.f_clinic_no = u'儿科'
    sdo.s_clinic_no = u'小儿科'
    sdo.data_source = 'cc'

    sdo.search_pv = 10
    sdo.search_uv = 1
    sdo.search_sum = 100
    sdo.search_avg = 0.24

    sdo.click_pv = 10
    sdo.click_uv = 1
    sdo.click_sum = 100
    sdo.click_avg = 0.36

    sdo.purchase_pv = 10
    sdo.purchase_uv = 1
    sdo.purchase_sum = 100
    sdo.purchase_avg = 0.48

    sdo.s2c_uv_ratio = 0.12
    sdo.c2p_uv_ratio = 0.24
    sdo.s2p_uv_ratio = 0.36

    sdo.record_time = str_to_datetime('20170225')
    print sdo.info()
    save_db_row(sdo)
    print "\t测试对象完成!"


if __name__ == '__main__':
    # create_sl_table()
    # test_of_add_one_row()
    delete_db_row("2018-02-14")
