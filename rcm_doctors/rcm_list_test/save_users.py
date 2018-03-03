#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
存储用户ID数据

Created by C.L.Wang
"""
import collections
import os
from datetime import datetime

import happybase

from configs.configs import HBaseConfigs
from project_utils import write_line, create_file, list_2_utf8
from rcm_doctors.constants import RCM_LIST_DATA
from rcm_doctors.rcm_list_test.user_types import is_low_intent_v2, is_paid_avg_more10
from remote_access.remote_apis import get_user_intention
from root_dir import ROOT_DIR

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "configs.settings")


def save_users(start_day, end_day, saved_file):
    """
    存储用户
    :param start_day: 起始时间
    :param end_day: 终止时间
    :param saved_file: 存储文件名
    :return: 
    """
    print "\t起始时间: %s, 终止时间: %s" % (start_day, end_day)
    create_file(saved_file)
    sd_time = date_2_str(start_day)
    ed_time = date_2_str(end_day)
    connection = happybase.Connection(host=HBaseConfigs.h3_host, port=HBaseConfigs.hbase_port)
    table = connection.table(HBaseConfigs.search_log_table)
    rows = table.scan(row_start=sd_time, row_stop=ed_time, batch_size=2000, columns=['info:search_uid'])

    count = 0  # 总行数
    e_count = 0  # 空行数
    user_set = set()
    for row_key, row_data in rows:
        uid = row_data.get("info:search_uid")
        if not uid:
            e_count += 1  # uid为空的数
            continue
        user_set.add(uid)

        if count % 1000 == 0:
            print "\tcount %s" % count
        count += 1

    print "\tUID数: %s, 总行数: %s, 空行数: %s" % (len(list(user_set)), count, e_count)

    user_list = sorted(list(user_set))
    user_type_dict = collections.defaultdict(int)
    user_type_list = list()

    count = 0  # 总行数
    for uid in user_list:
        type = str(get_user_type_v2(uid))
        uid_info = [uid, type]
        user_type_dict[type] += 1
        user_type_list.append(uid_info)
        if count % 200 == 0:
            print "\tcount %s" % count
        count += 1

    print "用户意愿数据: %s" % list_2_utf8(user_type_dict)
    for uid_info in user_type_list:
        write_line(saved_file, ','.join(uid_info))

    print "\t写入文件完成!!!"


def get_user_type(uid):
    """
    获取用户类型
    :param uid: 
    :return: 
    """
    is_low = is_low_intent_v2(uid, False)  # 低付费意愿
    is_high = is_paid_avg_more10(uid, False)  # 高付费意愿
    if is_low:
        return 1
    elif is_high:
        return 2
    else:
        return 0


def get_user_type_v2(uid):
    """
    获取用户类型
    :param uid: 
    :return: 意愿
    """
    return get_user_intention(uid)


def date_2_str(date_str, date_format='%Y%m%d%H%M%S'):
    date = datetime.strptime(date_str, '%Y%m%d')
    return date.strftime(date_format)


def test_of_save_users():
    start_day = '20180115'
    end_day = '20180116'
    saved_file = os.path.join(ROOT_DIR, RCM_LIST_DATA, 'users_%s_%s' % (start_day, end_day))
    save_users(start_day, end_day, saved_file)


if __name__ == '__main__':
    test_of_save_users()
