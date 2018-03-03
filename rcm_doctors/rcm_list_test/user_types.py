#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""


Created by C.L.Wang
"""
import time
from datetime import timedelta

from remote_access.remote_apis import get_user_paid_avg


def is_low_intent_v2(uid, log=False):
    """
    低付费医院
    :param uid: UID 
    :param log: 日志
    :return: 是否低付费医院
    """
    is_low = False  # 是否是低付费意愿
    s_time = time.time()
    price_num, price_avg = get_user_paid_avg(uid)  # 数据来源MySQL，较慢
    if 0 < price_avg < 6:  # 第一类
        is_low = True
    if log:
        print "\tID %s，耗时 %s，客单价: %s, 付费意愿: %s" % \
              (uid, timedelta(seconds=time.time() - s_time), price_avg, "低" if is_low else "未知")
    return is_low


def is_paid_avg_more10(uid, log=False):
    """
    高付费意愿
    :param uid: UID 
    :param log: 日志
    :return: 高付费意愿
    """
    [paid_num, paid_avg] = get_user_paid_avg(uid)
    is_high = paid_num >= 2 and paid_avg >= 10.0
    if log:
        print "\tID %s，耗时 %s，客单价: %s, 付费意愿: %s" % (uid, paid_num, paid_avg, "高" if is_high else "未知")
    return is_high
