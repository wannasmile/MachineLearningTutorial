#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created by C.L.Wang

import os
import sys

import pysolr

from rcm_doctors.clinic_const import ClinicUtils
from rcm_doctors.constants import RCM_LIST_DATA
from root_dir import ROOT_DIR

p = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if p not in sys.path:
    sys.path.append(p)

from configs.configs import SolrConfigs
from project_utils import *


def search_doctors_by_sort(sort_type, rows=20):
    """
    根据排序类型不同, 获取医生列表
    :param sort_type: 类型
    :param rows: 列
    :return: 20个医生的列表
    """
    solr = pysolr.Solr(SolrConfigs.TEST_SOLR_DOCTORS)  # 测试

    if sort_type == 'recommend':
        sort_str = "recommend_rate desc,score desc,cust_star_p desc"
    elif sort_type == 'reply_num':
        sort_str = "reply_num desc,score desc,cust_star_p desc"
    elif sort_type == 'speed':
        sort_str = "speed_score desc,score desc,cust_star_p desc"
    else:
        sort_str = "score desc,cust_star_p desc"

    query = '*:*'
    search_params = {'rows': rows, 'sorts': '%s' % sort_str, 'fl': '*, score'}  # 只输出几个内容

    result = solr.search(query, **search_params)

    ids_weights = list()
    name_list = list()
    clinic_list = list()
    price_list = list()

    for doctor in result.docs:
        id = doctor['id']
        weight = get_doctor_weight(doctor['cust_star'], doctor['activity'])  # 计算权重
        ids_weights.append((id, weight))  # 医生ID和权重
        name_list.append(doctor['name'])  # 名字
        clinic_list.append(doctor['clinic_no'])
        if 'pro_price' in doctor:  # 图文价格
            price_list.append(doctor['pro_price'])
        else:
            price_list.append(0)  # 可能未开通图文

    return ids_weights, name_list, clinic_list, price_list


def get_doctor_weight(cust_star, activity):
    """
    获取医生权重，推荐算法使用，来源于医生搜索
    :param cust_star: 推荐得分 
    :param activity: 活跃度
    :return: 权重
    """
    weight = 1.
    if cust_star <= 5000:
        weight *= 0.2
    elif cust_star <= 6500:
        weight *= 0.5
    elif cust_star <= 8000:
        weight *= 0.9
    if activity <= 50:
        weight *= 0.5
    elif activity <= 80:
        weight *= 0.8
    return weight


def search_doctors_by_filter(filter_type, rows=20):
    """
    根据过滤条件不同, 获取医生列表
    :param filter_type: 过滤类型
    :param rows: 列
    :return: 20个医生的列表
    """
    solr = pysolr.Solr(SolrConfigs.TEST_SOLR_DOCTORS)  # 测试

    if filter_type == 'clinic':
        fq_str = "clinic_no:3"
    elif filter_type == 'city':
        fq_str = "city:上海市"
    elif filter_type == 'service':
        fq_str = "tel_price:*"
    elif filter_type == 'price':
        fq_str = "pro_price:[11 TO 30]"
    elif filter_type == 'title':
        fq_str = "title:*主任医师*"
    else:
        return None

    query = '*:*'
    search_params = {'rows': rows, 'fq': '%s' % fq_str, 'sorts': 'score desc,cust_star_p desc',
                     'fl': '*, score'}  # 只输出几个内容
    result = solr.search(query, **search_params)
    doctors = result.docs

    ids_weights = list()
    name_list = list()
    clinic_list = list()
    price_list = list()

    for doctor in doctors:
        id = doctor['id']
        weight = get_doctor_weight(doctor['cust_star'], doctor['activity'])  # 计算权重
        ids_weights.append((id, weight))  # 医生ID和权重
        name_list.append(doctor['name'])  # 名字
        clinic_list.append(doctor['clinic_no'])  # 科室
        if 'pro_price' in doctor:  # 图文价格
            price_list.append(doctor['pro_price'])
        else:
            price_list.append(0)  # 可能未开通图文

    return ids_weights, name_list, clinic_list, price_list


DISEASE_LIST = ["腹痛", "感冒", "高血压", "糖尿病", "肿瘤", "阴道炎", "流产", "冠心病", "腹泻", "外伤"]
SYMPTOM_LIST = ["出血", "过敏", "月经", "怀孕", "紧张", "咳嗽", "发热", "发烧", "出汗", "囊肿"]


def search_doctors_by_word(word_type, word, rows=20):
    """
    根据单词不同, 获取医生列表
    :param word_type: 词类型
    :param word: 词
    :param rows: 列数
    :return: 20个医生的列表
    """
    solr = pysolr.Solr(SolrConfigs.TEST_SOLR_DOCTORS)  # 测试
    if word_type == 'disease':
        fq_str = "doctor_disease:*%s*" % word
    elif word_type == 'symptom':
        fq_str = "doctor_symptom:*%s*" % word
    else:
        return None

    query = '*:*'
    search_params = {'rows': rows, 'fq': '%s' % fq_str, 'sorts': 'score desc,cust_star_p desc',
                     'fl': '*, score'}  # 只输出几个内容
    result = solr.search(query, **search_params)
    doctors = result.docs

    ids_weights = list()
    name_list = list()
    clinic_list = list()
    price_list = list()

    for doctor in doctors:
        id = doctor['id']
        weight = get_doctor_weight(doctor['cust_star'], doctor['activity'])  # 计算权重
        ids_weights.append((id, weight))  # 医生ID和权重
        name_list.append(doctor['name'])  # 名字
        clinic_list.append(doctor['clinic_no'])  # 科室
        if 'pro_price' in doctor:  # 图文价格
            price_list.append(doctor['pro_price'])
        else:
            price_list.append(0)  # 可能未开通图文

    return ids_weights, name_list, clinic_list, price_list


CLINIC_LIST = [u'1', u'2', u'4', u'3', u'8', u'21', u'9', u'12',
               u'7', u'17', u'13', u'15', u'14', u'11', u'16', u'22',
               u'6']  # 过滤科室


def search_doctors_by_clinic(clinic_no, rows=20):
    """
    根据单词不同, 获取医生列表
    :param clinic_no: 词类型
    :param word: 词
    :param rows: 列数
    :return: 20个医生的列表
    """
    solr = pysolr.Solr(SolrConfigs.TEST_SOLR_DOCTORS)  # 测试

    query = 'clinic_no:%s' % clinic_no
    search_params = {'rows': rows, 'sorts': 'score desc,cust_star_p desc', 'fl': '*, score'}  # 只输出几个内容
    result = solr.search(query, **search_params)
    doctors = result.docs

    ids_weights = list()
    name_list = list()
    clinic_list = list()
    price_list = list()

    for doctor in doctors:
        id = doctor['id']
        weight = get_doctor_weight(doctor['cust_star'], doctor['activity'])  # 计算权重
        ids_weights.append((id, weight))  # 医生ID和权重
        name_list.append(doctor['name'])  # 名字
        clinic_list.append(doctor['clinic_no'])  # 科室
        if 'pro_price' in doctor:  # 图文价格
            price_list.append(doctor['pro_price'])
        else:
            price_list.append(0)  # 可能未开通图文

    return ids_weights, name_list, clinic_list, price_list


def save_clinic_file(folder_name, clinic):
    """
    存储科室文件
    :param folder_name: 文件夹
    :param clinic: 科室名
    :return: 
    """
    out_file = os.path.join(folder_name, ClinicUtils.clinic_no_to_name(clinic))  # 写入文件
    if os.path.exists(out_file):
        print "文件存在，删除文件：%s" % out_file
        os.remove(out_file)  # 删除已有文件
    ids_weights, names, clinics, prices = search_doctors_by_clinic(clinic)
    for ((did, weight), name, clinics, price) in zip(ids_weights, names, clinics, prices):
        line = "%s,%s,%s,%s,%s" % (did, name, weight, clinics, price)
        write_line(out_file, line)


def save_word_file(folder_name, word_type, word):
    """
    存储单词文件
    :param folder_name: 
    :param word_type: 
    :param word: 
    :return: 
    """
    out_file = os.path.join(folder_name, "%s_%s" % (word_type, word))  # 写入文件
    if os.path.exists(out_file):
        print "文件存在，删除文件：%s" % out_file
        os.remove(out_file)  # 删除已有文件
    ids_weights, names, clinics, prices = search_doctors_by_word(word_type, word)
    for ((did, weight), name, clinic, price) in zip(ids_weights, names, clinics, prices):
        line = "%s,%s,%s,%s,%s" % (did, name, weight, clinic, price)
        write_line(out_file, line)


def save_sort_file(folder_name, sort_type):
    """
    存储排序搜索的文件
    :param folder_name: 文件夹
    :param sort_type: 排序类型
    :return:
    """
    out_file = os.path.join(folder_name, "sort_%s" % sort_type)  # 写入文件
    if os.path.exists(out_file):
        print "文件存在，删除文件：%s" % out_file
        os.remove(out_file)  # 删除已有文件
    ids_weights, names, clinics, prices = search_doctors_by_sort(sort_type)
    for ((did, weight), name, clinics, price) in zip(ids_weights, names, clinics, prices):
        line = "%s,%s,%s,%s,%s" % (did, name, weight, clinics, price)
        write_line(out_file, line)


def save_filter_file(folder_name, filter_type):
    """
    存储排序搜索的文件
    :param folder_name: 文件夹
    :param filter_type: 过滤类型
    :return:
    """
    out_file = os.path.join(folder_name, "filter_%s" % filter_type)  # 写入文件
    if os.path.exists(out_file):
        print "文件存在，删除文件：%s" % out_file
        os.remove(out_file)  # 删除已有文件
    ids_weights, names, clinics, prices = search_doctors_by_filter(filter_type)
    for ((did, weight), name, clinic, price) in zip(ids_weights, names, clinics, prices):
        line = "%s,%s,%s,%s,%s" % (did, name, weight, clinic, price)
        write_line(out_file, line)


def save_doctors_set():
    """
    存储医生文件
    :return:
    """
    folder_dct = os.path.join(ROOT_DIR, RCM_LIST_DATA, 'doctors_%s' % get_current_time_str())

    folder_name = os.path.join(folder_dct, "sort")
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print '文件夹 "%s" 不存在，创建文件夹。' % folder_name
    save_sort_file(folder_name, 'recommend')
    save_sort_file(folder_name, 'reply_num')
    save_sort_file(folder_name, 'speed')

    folder_name = os.path.join(folder_dct, "filter")
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print '文件夹 "%s" 不存在，创建文件夹。' % folder_name
    save_filter_file(folder_name, 'clinic')
    save_filter_file(folder_name, 'city')
    save_filter_file(folder_name, 'price')
    save_filter_file(folder_name, 'service')
    save_filter_file(folder_name, 'title')

    folder_name = os.path.join(folder_dct, "disease")
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print '文件夹 "%s" 不存在，创建文件夹。' % folder_name
    for word in DISEASE_LIST:
        save_word_file(folder_name, 'disease', word)

    folder_name = os.path.join(folder_dct, "symptom")
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print '文件夹 "%s" 不存在，创建文件夹。' % folder_name
    for word in SYMPTOM_LIST:
        save_word_file(folder_name, 'symptom', word)

    folder_name = os.path.join(folder_dct, "clinic")
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print '文件夹 "%s" 不存在，创建文件夹。' % folder_name
    for clinic in CLINIC_LIST:
        save_clinic_file(folder_name, clinic)


if __name__ == '__main__':
    save_doctors_set()  # 存储医生数据
