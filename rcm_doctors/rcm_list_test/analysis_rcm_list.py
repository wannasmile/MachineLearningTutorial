#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
分析推荐的医生列表

Created by C.L.Wang
"""
import os
import sys

import xlsxwriter

p = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if p not in sys.path:
    sys.path.append(p)

from project_utils import *
from rcm_doctors.clinic_const import ClinicUtils
from rcm_doctors.constants import RCM_LIST_DATA
from remote_access.remote_apis import rank_doctors_by_mode
from root_dir import ROOT_DIR


class RecommendType(object):
    INCOME = u"income"
    PURCHASE = u"purchase"
    DEFAULT = u'default'


def analyze_rcm_list(uids_file, doctors_dir, res_file):
    """
    服务上线测试流程
    :param uids_file: uid文件
    :param doctors_dir: 科室医生文件夹 
    :param res_file: 写入的文件
    :return: 
    """
    print "运行测试低付费意愿中..."

    print "用户文件：%s" % uids_file
    print "医生文件夹：%s" % doctors_dir
    create_file(res_file)  # 创建文件
    workbook = xlsxwriter.Workbook(res_file)  # 创建Excel
    worksheet = workbook.add_worksheet()  # 添加Sheet

    start_time = time.time()
    category_doctors, id_name_dict, id_clinic_dict, id_price_dict = get_dct_info(doctors_dir)
    uid_dict = get_usr_info(uids_file)

    print "\t排序医生列表中..."

    excel_count = 0  # 用于控制行数
    all_price_dict = collections.defaultdict(int)  # 价格字典
    # file_count = 0  # 用于小批量测试
    for (file_name, ids_weights) in category_doctors:  # 使用List，避免排序
        print "\t处理文件: %s" % file_name
        did_count_dict = dict()  # 医生的字典: 推荐类别, 推荐位置, 医生ID
        count = 0
        for uid in uid_dict.keys():
            try:
                doctor_list = json.loads(rank_doctors_by_mode(uid, ids_weights, uid_dict[uid]))['data']
            except Exception as e:
                print "\tException: 推荐医生服务异常!!!"
                continue
            if not doctor_list:  # 异常时，返回空数据
                continue

            for index in range(len(doctor_list)):
                doctor_info = doctor_list[index]
                doctor_id = doctor_info[0]  # 医生ID
                user_type = uid_dict[uid]  # 用户类型
                if user_type not in did_count_dict:
                    did_count_dict[user_type] = dict()
                if index not in did_count_dict[user_type]:
                    did_count_dict[user_type][index] = collections.defaultdict(int)
                did_count_dict[user_type][index][doctor_id] += 1
                if index == 2:  # 统计前3位
                    break
            count += 1
            if count % 500 == 0:
                print "\tcount %s" % count
                # break
        excel_count, type_dict = save_rcm_dict(did_count_dict, excel_count, file_name,
                                               id_name_dict, id_clinic_dict, id_price_dict,
                                               worksheet)  # 存储推荐列表的字典数据
        all_price_dict = merge_type_price_dict(all_price_dict, type_dict)  # 汇总数据
        # 用于测试文件数量
        # file_count += 1
        # if file_count == 2:
        #     break

    worksheet = workbook.add_worksheet()  # 添加新的Sheet
    save_all_price_dict(all_price_dict, worksheet)  # 存储评价价格统计

    workbook.close()
    print "运行测试低付费意愿完成! 耗时: %s" % (timedelta(seconds=time.time() - start_time))


def save_all_price_dict(all_price_dict, worksheet):
    """
    存储总体的价格数据
    :param all_price_dict: 总体的价格字典
    :param worksheet: 工作表
    :return: 写入文件
    """
    worksheet.write_row(0, 0, [u'推荐方式', u'位置', u'推荐次数', u'平均价格'])
    count = 1
    for user_type in all_price_dict:
        for index in all_price_dict[user_type]:
            all_show, all_count, all_turn = all_price_dict[user_type][index]
            data_list = [unicode(user_type), unicode(index + 1), unicode(all_count),
                         "%0.2f" % safe_div(safe_div(all_show, all_turn), all_count)]  # 数据列表
            worksheet.write_row(count, 0, data_list)
            count += 1


def merge_type_price_dict(all_price_dict, type_dict):
    """
    合并统计字典
    :param all_price_dict: 所有数据的统计字典
    :param type_dict: 类型字典
    :return: 统计字典
    """
    for user_type in type_dict.keys():
        if user_type not in all_price_dict:  # 初始化汇总的字典
            all_price_dict[user_type] = dict()
        for index in type_dict[user_type].keys():

            sum_show, sum_count = type_dict[user_type][index]
            if index not in all_price_dict[user_type]:  # 初始化汇总的字典
                all_price_dict[user_type][index] = (0, sum_count, 0)  # 总展示, 总数, 轮次

            # 更新数据
            all_show, all_count, all_turn = all_price_dict[user_type][index]
            all_show += sum_show
            all_turn += 1
            all_price_dict[user_type][index] = (all_show, all_count, all_turn)

    return all_price_dict


def save_rcm_dict(res_dict, count, type_name, id_name_dict, id_clinic_dict, id_price_dict, worksheet):
    type_dict = dict()
    for user_type in res_dict.keys():
        index_avg = dict()
        if count == 0:  # 只使用第一行
            worksheet.write_row(count, 0, [u'数据来源', u'推荐方式', u'位置', u'ID', u'姓名', u'科室', u'推荐次数', u'图文价格'])
            count += 1
        for index in res_dict[user_type].keys():
            sum_show = 0
            sum_count = 0
            did_dict = res_dict[user_type][index]
            did_sorted_list = sort_dict_by_value(did_dict)
            for (did, did_num) in did_sorted_list:
                clinic_name = ClinicUtils.clinic_no_to_name(id_clinic_dict[did])
                data_list = [unicode(type_name), unicode(user_type), unicode(index + 1), unicode(did),
                             unicode(id_name_dict[did]),
                             unicode(clinic_name), unicode(did_num),
                             unicode(id_price_dict[did])]
                sum_show += (int(id_price_dict[did]) * did_num)  # 总价
                sum_count += did_num  # 总数
                worksheet.write_row(count, 0, data_list)
                count += 1
                if count % 200 == 0:
                    print "count %s" % count
            index_avg[index] = (sum_show, sum_count)
            # count = save_index_sum(user_type, index_avg, worksheet, count)
        type_dict[user_type] = index_avg
    print "\t写入文件完成! 行数: %s" % count
    return count, type_dict


def save_index_sum(user_type, index_avg, worksheet, count):
    worksheet.write_row(count, 0, [])
    count += 1
    worksheet.write_row(count, 0, [u'推荐方式', u'位置', u'均价', u'总价', u'次数'])
    count += 1
    for index in index_avg.keys():
        (sum_show, sum_count) = index_avg[index]
        data_list = [unicode(user_type), unicode(index + 1), unicode('%0.2f' % safe_div(sum_show, sum_count)),
                     unicode(sum_show), unicode(sum_count)]
        worksheet.write_row(count, 0, data_list)
        count += 1
    worksheet.write_row(count, 0, [])
    count += 1
    return count


def get_dct_info(doctors_dir, out_excel=False):
    """
    获取医生信息
    :param doctors_dir: 医生文件夹
    :param out_excel: 输出的Excel
    :return: 医生信息
    """
    print "\t读取医生列表中..., 文件夹: %s" % doctors_dir
    file_paths, file_names = listdir_files(doctors_dir)

    category_doctors = list()  # 类别->推荐医生
    id_name_dict = dict()  # id->名字
    id_price_dict = dict()  # id->图文价格
    id_clinic_dict = dict()  # id->科室

    if out_excel:
        print '\t写入Excel文件!'
        excel_file = "%s.xlsx" % doctors_dir
        create_file(excel_file)  # 创建文件
        workbook = xlsxwriter.Workbook(excel_file)  # 创建Excel
        worksheet = workbook.add_worksheet()  # 添加Sheet
        worksheet.write_row(0, 0, [u'数据来源', u'位置', u'ID', u'姓名', u'科室', u'图文价格'])
        excel_count = 1

    for (file_path, file_name) in zip(file_paths, file_names):
        ids_weights = []  # 医生id和权重
        lines = read_file(file_path)
        doctor_count = 0
        for line in lines:
            # clinic_web_04e8f9c3b9b883b2,赵勇,1.0,30
            [did, name, weight, clinic, price] = line.split(",")  # 分解
            ids_weights.append((did, float(weight)))  # 必须要是float
            id_name_dict[did] = name  # 医生名字
            id_clinic_dict[did] = clinic  # 医生科室
            id_price_dict[did] = price  # 医生图文价格
            doctor_count += 1

            if out_excel:
                clinic_name = ClinicUtils.clinic_no_to_name(clinic)
                data_list = [unicode(file_name), unicode(doctor_count), unicode(did),
                             unicode(name), unicode(clinic_name), unicode(price)]
                worksheet.write_row(excel_count, 0, data_list)  # 写入数据文案
                excel_count += 1

            if doctor_count == 20:  # 每个科室只取前20个
                break
        # category_doctors[unicode(file_name)] = ids_weights  # 科室信息
        category_doctors.append((unicode(file_name), ids_weights))  # 科室信息

    if out_excel:
        print "写入行数: %s" % excel_count
    print "\t读取医生列表完成! "
    return category_doctors, id_name_dict, id_clinic_dict, id_price_dict


def get_usr_info(uids_file):
    """
    读取医生文件
    :param uids_file: 医生文件 
    :return: 医生列表
    """
    user_infos = read_file(uids_file)
    user_dict = dict()
    for user_info in user_infos:
        uid, user_type = user_info.split(',')
        if user_type == '1':  # 低付费
            user_dict[uid] = RecommendType.PURCHASE
        elif user_type == '2':  # 高付费
            user_dict[uid] = RecommendType.INCOME
        else:  # 默认
            user_dict[uid] = RecommendType.DEFAULT
    return user_dict


def test_of_get_dct_info():
    doctors_dir = os.path.join(ROOT_DIR, RCM_LIST_DATA, 'doctors_20180131144055')  # 医生信息
    get_dct_info(doctors_dir, True)  # 写入Excel


def test_of_analyze_rcm_list():
    """
    测试推荐列表
    :return: 输出Excel统计文件
    """
    uid_file = os.path.join(ROOT_DIR, RCM_LIST_DATA, 'users_20180115_20180116_new')  # 用户信息
    doctors_dir = os.path.join(ROOT_DIR, RCM_LIST_DATA, 'doctors_20180131144055')  # 医生信息
    res_file = os.path.join(ROOT_DIR, RCM_LIST_DATA,
                            "result_of_rcm_doctors_%s.xlsx" % get_current_time_str())  # 输出的Excel
    analyze_rcm_list(uid_file, doctors_dir, res_file)


if __name__ == '__main__':
    # test_of_get_dct_info()  # 测试获取医生信息
    test_of_analyze_rcm_list()
