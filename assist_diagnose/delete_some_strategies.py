#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""


Created by C.L.Wang
"""
import os

import sys

p = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if p not in sys.path:
    sys.path.append(p)

from django.db import connections

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "configs.settings")

remove_list = [
    "男科_2_外生殖器",
    "男科_1_泌尿",
    "男科_3_性功能障碍",
    "皮肤科_3_非痤疮",
    "外科_2_前列腺",
    "男科_5_疲劳",
    "儿科_1_发热",
    "外科_7_痔疮",
    "妇科_4_怀孕",
    "外科_1_排尿异常",
    "内科_13_血压",
    "眼科_2_感觉异常",
    "外科_5_性功能障碍",
    "内科_5_腹痛",
    "口腔科_1_牙周",
    "内科_14_糖尿病",
    "内科_11_心悸",
    "口腔科_2_牙体牙髓",
    "外科_6_胆系疾病"]


def delete_some_strategies(item_str):
    cursor = connections['10_215_33_152_bigdata'].cursor()
    sql = "SELECT * FROM assistant_diagnose_strategy WHERE strategy_name = \"%s\"" % item_str
    print "\t执行语句：%s" % sql
    cursor.execute(sql)
    rows = cursor.fetchall()
    final_rows = [list(i) for i in rows]
    for row in final_rows:
        print "res: \n%s" % row
    sql = "DELETE FROM assistant_diagnose_strategy WHERE strategy_name = \"%s\"" % item_str
    print "\t执行语句：%s" % sql
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        print "res: \n%s" % row


if __name__ == '__main__':
    for item_str in remove_list:
        delete_some_strategies(item_str)
