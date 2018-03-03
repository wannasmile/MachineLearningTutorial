#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
关于购买信息，来源于online_medical的api_paymentorder表中。
HBase数据 - 点击事件与搜索事件的关联：小于5分钟；购买事件与搜索事件的关联：小于5个小时；
HBase数据在search_log_analysis工程中每日同步；
将HBase数据，同步至搜索日志中。

Created by C.L.Wang
"""
import json
import re
from datetime import datetime

import happybase

from configs.configs import HBaseConfigs
from project_utils import datetime_to_str, FORMAT_DATE_3


def get_hbase_data(start_str, end_str):
    """
    获取HBase的数据
    :param start_str: 起始时间-20180122
    :param end_str: 结束时间-20180123
    :return: 处理数据
    """
    start_time = datetime.strptime(start_str, '%Y%m%d')  # 起始日期
    end_time = datetime.strptime(end_str, '%Y%m%d')  # 结束日期
    rows = scan_data(start_time, end_time)  # 行数据
    processed_data = {}  # 处理数据

    for row_key, data in rows:
        row_data = {}
        cur_purchase_infos = []
        cur_click_infos = []
        for cf_qualifier, value in data.items():
            if is_purchase_info(cf_qualifier):  # 购买信息
                purchase_info = json.loads(value)
                cur_purchase_infos.append(
                    (purchase_info["doctor_id"], purchase_info["created_timestamp"], purchase_info))
            elif is_click_info(cf_qualifier):  # 点击信息
                click_doctor_id = extract_click_doctor_id(DH_CLICK_PATTERN, cf_qualifier)[0]
                cur_click_infos.append((click_doctor_id, value))
            elif cf_qualifier == "info:search_query":
                row_data["query"] = value  # 搜索内容
            elif cf_qualifier == "info:search_uid":
                row_data["uid"] = value  # 用户ID
            elif cf_qualifier == "info:search_sort":
                row_data["sort"] = value  # 排序
            elif cf_qualifier == "info:search_device_id":
                row_data["device_id"] = value  # 设备ID
            elif cf_qualifier == "info:search_clinic_no":
                row_data["clinic_no"] = value  # 科室号
            elif cf_qualifier == "info:search_user_input":
                row_data["user_input"] = json.loads(value)  # 用户输入
            elif cf_qualifier == "info:search_result":
                row_data["search_result"] = get_simple_search_result(json.loads(value))  # 搜索结果
        row_data["purchase_infos"] = cur_purchase_infos  # 添加购买信息
        row_data["click_infos"] = cur_click_infos  # 添加点击信息
        processed_data[row_key] = row_data  # 添加数据

    return processed_data


DH_CLICK_PATTERN = re.compile(r"info:doctor_home_click_(\w+)")  # 医生主页点击的正则


def extract_click_doctor_id(pattern, input_str):
    """
    用于解析医生的ID
    info:doctor_home_click_clinic_web_d138ce8d66cd186d
    :param pattern: 正则
    :param input_str: 输入数据
    :return: 
    """
    return pattern.findall(input_str)


def is_purchase_info(cf_qualifier):
    """
    判断是否是购买信息
    info:purchase_clinic_web_998297400c8490ad
    :param cf_qualifier: 列族 
    :return: 是否
    """
    if cf_qualifier.startswith("info:purchase_"):
        return True
    return False


def is_click_info(cf_qualifier):
    """
    判断是否是点击信息
    info:doctor_home_click_clinic_web_57f9312890a59fba
    :param cf_qualifier: 列族
    :return: 是否
    """
    if cf_qualifier.startswith("info:doctor_home_click_"):
        return True
    return False


def get_simple_search_result(result):
    """
    封装医生的数据
    :param result: 数据结果
    :return: 医生数据
    """
    need_key = ["name", "id", "title", "tel_price", "cust_star", "pro_price",
                "second_class_clinic_no", "clinic_no", "reply_num", "hospital_name", "recommend_index",
                "model_type"]
    for doctor_info in result:
        for k, v in doctor_info.items():
            if k not in need_key:
                del doctor_info[k]
    return result


def scan_data(start_day, end_day, filter_str=None, columns=None, limit=None):
    """
    获取HBase的数据
    :param start_day: 起始时间 
    :param end_day: 终止时间
    :param filter_str: 
    :param columns: 
    :param limit: 
    :return: 
    """
    start_day_str = datetime_to_str(start_day, FORMAT_DATE_3)  # 起始时间
    end_day_str = datetime_to_str(end_day, FORMAT_DATE_3)  # 终止时间
    # start_day_str = "20171108180000" # 测试
    # end_day_str = "20171109180000" # 测试
    connection = happybase.Connection(host=HBaseConfigs.h3_host, port=HBaseConfigs.hbase_port)  # 测试
    table = connection.table(HBaseConfigs.search_log_table)  # 表
    # rows = table.scan(row_start=start_day_str, row_stop=end_day_str, filter=filter_str, batch_size=2000)
    rows = table.scan(row_start=start_day_str, row_stop=end_day_str, filter=filter_str, batch_size=2000, limit=400)
    return rows


if __name__ == '__main__':
    print get_hbase_data("20180111", "20180112")
