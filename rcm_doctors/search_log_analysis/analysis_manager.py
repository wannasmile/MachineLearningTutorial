#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created by C.L.Wang
import os
import sys

p = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if p not in sys.path:
    sys.path.append(p)

from rcm_doctors.search_log_analysis.data_loader import get_hbase_data
from rcm_doctors.search_log_analysis.search_log_db_api import delete_db_row, save_db_row

from project_utils import *
from rcm_doctors.clinic_const import ClinicUtils
from rcm_doctors.search_log_analysis.models import SearchDoctorsObject


class RcmType(object):
    INCOME = u"income"
    INCOME_OLD = u"gbdt"
    PURCHASE = u"purchase"
    DEFAULT = u'default'

    INCOME_SET = [INCOME, INCOME_OLD]  # 就的版本


ROBOT_IDS = [u'101639648', u'54253107']  # 机器人账号，异常


def analysis_logs(processed_data):
    """
    修改处理数据的接口
    :param processed_data: 处理数据
    :return: 
    """
    uid_data_dict = dict()  # user_id -> 多条记录

    search_total = collections.defaultdict(int)  # 搜索整体数据
    total_data_dict = dict()  # 分条数据统计

    for row_key, data in processed_data.items():
        user_id = data["uid"]
        if not user_id:
            search_total['no_uid'] += 1  # 统计不含有用户ID的数量
            continue
        cur_data = uid_data_dict.setdefault(user_id, {})  # 每个device_id含有多条搜索记录
        cur_data[row_key] = data

    log_count = 0  # 用于统计日志
    s_time = time.time()
    print "\t总用户数：%s" % len(uid_data_dict.items())
    uid_click_set = set()  # 使用集合统计点击UV
    uid_purchase_set = set()  # 使用集合购买UV

    for user_id, s_items in uid_data_dict.items():
        search_total["show_times"] += 1  # 搜索的全部UV

        # print "\t\t用户行数：%s" % len(s_items.items())
        for row_key, s_item in s_items.items():  # 遍历搜索项
            if log_count % 10000 == 0:
                print "\t\tcount %s, time %s" % (log_count, timedelta(seconds=time.time() - s_time))
            log_count += 1

            query = s_item["query"]  # 搜索词
            clinic_no = s_item["clinic_no"]  # 搜索科室
            search_result = s_item["search_result"]  # 搜索结果
            user_input = s_item["user_input"]  # 用户输入信息
            uid = s_item.get("uid", None)  # 用户ID

            user_filter = user_input.get("filter", {})  # 过滤信息，点击科室属于这类
            query_filter = user_input.get("query", {})  # 查询信息

            query_clinic_no = user_filter.get('clinic_no', None)  # 用户的搜索科室
            query_text = query_filter.get("text", None)  # 搜索文案
            sort = s_item.get("sort", "default")  # 排序方式，默认default
            start_row = query_filter.get("start", -1)  # 起始位置

            if uid in ROBOT_IDS:  # 过滤掉机器人账号
                continue

            click_infos = s_item["click_infos"]  # 点击信息
            purchase_infos = s_item["purchase_infos"]  # 购买信息

            if not search_result or (not is_recommend_item(uid, start_row)):
                search_total["unrecommend_num"] += 1
                continue  # 不含搜索和非推荐的直接略过
            search_total["recommend_num"] += 1

            from_type = query_filter.get("from_type")  # 来源u'zhaoyisheng'，一般都是找医生

            data_source = get_data_source(query_clinic_no, query_text, sort, user_filter)  # 获取数据来源
            data_type = get_user_type(search_result)  # 获取用户类型

            if click_infos:  # 含有点击信息用户的标记，用于统计UV
                uid_click_set.add(uid)
            if purchase_infos:  # 含有购买信息用户的标记，用于统计UV
                uid_purchase_set.add(uid)

            # 用于测试，等于展示PV
            for one_doctor in search_result:
                if "recommend_index" in one_doctor:
                    search_total["recommend_index"] += 1
                    break

            for t_idx in range(len(search_result)):
                target = search_result[t_idx]
                f_clinic_no, s_clinic_no, pro_price = get_doctor_data(target)  # 获取医生的科室和图文
                list_idx = str(t_idx + 1) if t_idx <= 2 else 'o'  # 只区分1~3，其他是o
                data_key = (data_type, data_source, list_idx, f_clinic_no, s_clinic_no)
                if data_key not in total_data_dict:
                    total_data_dict[data_key] = [[0.0] * 6, set(), set(), set()]
                update_search_item(total_data_dict[data_key], uid, pro_price)  # 更新信息
                # print "%s: %s, %s" % (data_key, total_data_dict[data_key][0], total_data_dict[data_key][1])

            if click_infos:  # 点击模块
                for (clinic_dct, _) in click_infos:
                    for t_idx in range(len(search_result)):
                        target = search_result[t_idx]
                        doctor_id = target.get('id', None)
                        if clinic_dct == doctor_id:
                            f_clinic_no, s_clinic_no, pro_price = get_doctor_data(target)  # 获取医生的科室和图文
                            list_idx = str(t_idx + 1) if t_idx <= 2 else 'o'  # 只区分1~3，其他是o
                            data_key = (data_type, data_source, list_idx, f_clinic_no, s_clinic_no)
                            if data_key not in total_data_dict:
                                total_data_dict[data_key] = [[0.0] * 6, set(), set(), set()]
                            update_search_item(total_data_dict[data_key], uid, pro_price, 'click')  # 更新信息
                            # print "%s: %s, %s, %s" % (data_key, total_data_dict[data_key][0],
                            #                           total_data_dict[data_key][1], total_data_dict[data_key][2])

            if purchase_infos:  # 购买模块
                for (purchase_dct, _, purchase_detail) in purchase_infos:
                    for t_idx in range(len(search_result)):
                        target = search_result[t_idx]
                        doctor_id = target.get('id', None)
                        if purchase_dct == doctor_id:
                            f_clinic_no, s_clinic_no, pro_price = get_doctor_data(target)  # 获取医生的科室和图文
                            list_idx = str(t_idx + 1) if t_idx <= 2 else 'o'  # 只区分1~3，其他是o
                            data_key = (data_type, data_source, list_idx, f_clinic_no, s_clinic_no)
                            if data_key not in total_data_dict:
                                total_data_dict[data_key] = [[0.0] * 6, set(), set(), set()]
                            update_search_item(total_data_dict[data_key], uid, pro_price, 'purchase')  # 更新信息
                            # print "%s: %s, %s, %s, %s" % (data_key, total_data_dict[data_key][0],
                            #                               total_data_dict[data_key][1],
                            #                               total_data_dict[data_key][2],
                            #                               total_data_dict[data_key][3])

    return search_total, total_data_dict


def is_recommend_item(uid, start_row):
    """
    逻辑参考search-server服务的SearchDoctorWorker#isNeedRecommend
    含有搜索科室，不含搜索文字，排序默认，起始位置是0，才能进入推荐算法程序
    :param uid: 用户ID
    :param query_clinic_no: 搜索科室
    :param query_text: 搜索文字
    :param sort: 排序方式
    :param start_row: 起始位置
    :return: 是否是推荐项
    """
    try:
        start_row_i = int(start_row)
    except Exception as e:
        print "\tstart_row error: %s" % e
        return False
    if uid and start_row_i == 0:
        return True
    else:
        return False


def get_data_source(query_clinic_no, query_text, sort, user_filter):
    """
    数据来源
    :param query_clinic_no: 点击科室
    :param query_text: 搜索词
    :param sort: 排序
    :param user_filter: 过滤 
    :return: 具体来源
    """
    user_filter.pop('is_tel_price_v2', None)
    if sort != "default":
        return "so"  # 排序sort
    elif user_filter:
        return "fi"  # 过滤Filter
    elif query_clinic_no:
        return "cc"  # 点科室click_clinic
    elif query_text:
        return "sw"  # 搜索词search_word


def get_user_type(search_result):
    """
    获取用户的类型，在RcmType中选择
    :param search_result: 搜索结果
    :return: 用户类型
    """
    for target in search_result:  # 展示模块
        model_type = target.get('model_type', None)  # 用户模式
        if model_type in RcmType.INCOME_SET:
            return RcmType.INCOME
        elif model_type == RcmType.PURCHASE:
            return RcmType.PURCHASE
    return RcmType.DEFAULT


def update_search_item(data_value, uid, pro_price, event_type='search'):
    """
    根据类型的不同，更新不同的数据
    :param data_value: 数据集
    :param uid: 用户ID，用于统计UV
    :param pro_price: 图文价格
    :param event_type: 事件类型search，click，purchase
    :param once: 用于搜索中
    :return: 更新项目
    """
    [search_pv, search_sum, click_pv, click_sum, purchase_pv, purchase_sum] = data_value[0]
    if event_type == 'search':
        search_set = data_value[1]
        search_pv += 1.0  # 更新数据
        search_sum += pro_price
        search_set.add(uid)
        data_value[1] = search_set
    elif event_type == 'click':
        click_set = data_value[2]
        click_pv += 1.0  # 更新数据
        click_sum += pro_price
        click_set.add(uid)
        data_value[2] = click_set
    elif event_type == 'purchase':
        purchase_set = data_value[3]
        purchase_pv += 1.0  # 更新数据
        purchase_sum += pro_price
        purchase_set.add(uid)
        data_value[3] = purchase_set
    data_value[0] = [search_pv, search_sum, click_pv, click_sum, purchase_pv, purchase_sum]


def get_doctor_data(doctor_info):
    """
    获取医生的信息
    :param doctor_info: 用户信息
    :return: 一级科室，二级科室，图文价格
    """
    f_clinic_no = doctor_info.get('clinic_no', None)
    s_clinic_no = doctor_info.get('second_class_clinic_no', None)
    pro_price = doctor_info.get('pro_price', None)  # 图文价格
    if pro_price < 0.0:
        pro_price = 0.0  # 存在图文小于1的情况
    return f_clinic_no, s_clinic_no, pro_price


def save_2_db(total_data_dict, data_str):
    for data_key in total_data_dict.keys():
        data_value = total_data_dict[data_key]
        sdo = SearchDoctorsObject()
        (sdo.data_type, sdo.data_source, sdo.pos_of_list, f_clinic, s_clinic) = data_key
        sdo.f_clinic_no = ClinicUtils.clinic_no_to_name(f_clinic)
        sdo.s_clinic_no = ClinicUtils.clinic_no_to_name(s_clinic)

        sdo.search_uv = len(data_value[1])
        sdo.search_pv = int(data_value[0][0])
        sdo.search_sum = int(data_value[0][1])

        sdo.click_uv = len(data_value[2])
        sdo.click_pv = int(data_value[0][2])
        sdo.click_sum = int(data_value[0][3])

        sdo.purchase_uv = len(data_value[3])
        sdo.purchase_pv = int(data_value[0][4])
        sdo.purchase_sum = int(data_value[0][5])

        sdo.record_time = str_to_datetime(data_str)
        sdo.info()

        save_db_row(sdo)  # 写入数据库


def sync_to_mysql(start_day_str, end_day_str, time_delta=24):
    start_day = str_to_datetime(start_day_str, FORMAT_DATE)
    end_day = str_to_datetime(end_day_str, FORMAT_DATE)
    cur_end_day = start_day + timedelta(hours=time_delta)
    cur_end_day = cur_end_day if cur_end_day < end_day else end_day
    while start_day < end_day:
        print "起始时间: %s, 结束时间: %s, 当前时间: %s" % \
              (datetime_to_str(start_day), datetime_to_str(end_day), datetime_to_str(cur_end_day))
        delete_db_row(datetime_to_str(start_day, FORMAT_DATE_2))  # 先删除再写入

        processed_data = get_hbase_data(datetime_to_str(start_day), datetime_to_str(end_day))  # 获取数据
        search_total, total_data_dict = analysis_logs(processed_data)  # 统计数据
        save_2_db(total_data_dict, datetime_to_str(start_day))  # 写入数据

        start_day = cur_end_day
        cur_end_day = cur_end_day + timedelta(hours=time_delta)


def test_of_analysis_logs(start_day_str, end_day_str):
    # start_time = "20180214"
    # processed_data = get_hbase_data(start_time, "20180215")
    # search_total, total_data_dict = analysis_logs(processed_data)
    # save_2_db(total_data_dict, start_time)
    sync_to_mysql(start_day_str, end_day_str)


if __name__ == '__main__':
    print '参数个数为:', len(sys.argv), '个参数。'
    print '参数列表:', str(sys.argv)
    if len(sys.argv) != 3:
        print "参数错误！参考：python analysis_manager.py 20180214 20180218"
        exit()
    test_of_analysis_logs(sys.argv[1], sys.argv[2])
