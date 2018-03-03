#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
对比不同的医生数据召回

Created by C.L.Wang
"""
import os

import pysolr
import xlsxwriter

from configs.configs import SolrConfigs
from project_utils import get_current_time_str
from rcm_doctors.clinic_const import FIRST_CLINICS, ClinicUtils
from rcm_doctors.constants import PRICE_FILTER_DATA
from root_dir import ROOT_DIR


def all_price_filter(clinic_no, price_list):
    solr = pysolr.Solr(SolrConfigs.TEST_SOLR_DOCTORS)
    q = "clinic_no:%s" % clinic_no
    price_fields = ['pro_price', 'tel_price_10', 'tel_price_15', 'tel_price_20', 'tel_price_30',
                    'register_price', 'fweek_price', 'fmonth_price', 'video_price', 'hospital_guide_price']
    filter_str = ''
    for pf in price_fields:
        filter_str += "%s:[%s TO %s] " % (pf, str(price_list[0]), str(price_list[1]))
    print "\t价格过滤： %s" % filter_str
    params = {"fl": "id,name", "rows": 10000, "sorts": "score desc,cust_star_p desc",
              "fq": [filter_str, "cust_star_p:[3000 TO *]"]}
    print "\t查询: %s" % q
    print "\t参数: %s" % params
    res = solr.search(q, **params)
    print "召回数: %s" % len(res.docs)
    return len(res.docs)


def min_price_filter(clinic_no, price_list):
    solr = pysolr.Solr(SolrConfigs.TEST_SOLR_DOCTORS)
    q = "clinic_no:%s" % clinic_no
    price_fields = ['pro_price', 'tel_price_10', 'tel_price_15', 'tel_price_20', 'tel_price_30',
                    'register_price', 'fweek_price', 'fmonth_price', 'video_price', 'hospital_guide_price']
    price_line = ",".join(price_fields)
    if str(price_list[1]) != '*':
        filter_str = "{!frange l=%s u=%s} min(%s)" % (str(price_list[0]), str(price_list[1]), price_line)
    else:
        filter_str = "{!frange l=%s} min(%s)" % (str(price_list[0]), price_line)

    params = {"fl": "id,name", "rows": 10000, "sorts": "score desc,cust_star_p desc",
              "fq": [filter_str, "cust_star_p:[3000 TO *]"]}
    print "\t查询: %s" % q
    print "\t参数: %s" % params
    res = solr.search(q, **params)
    print "召回数: %s" % len(res.docs)
    return len(res.docs)


def min_all_price_filter(clinic_no, price_list):
    solr = pysolr.Solr(SolrConfigs.TEST_SOLR_DOCTORS)
    q = "clinic_no:%s" % clinic_no

    price_fields = ['pro_price', 'register_price', 'video_price', 'hospital_guide_price',
                    'tel_price_min', 'fml_price_min']
    filter_str = ''
    for pf in price_fields:
        filter_str += "%s:[%s TO %s] " % (pf, str(price_list[0]), str(price_list[1]))
    print "\t价格过滤： %s" % filter_str
    params = {"fl": "id,name", "rows": 10000, "sorts": "score desc,cust_star_p desc",
              "fq": [filter_str, "cust_star_p:[3000 TO *]"]}
    print "\t查询: %s" % q
    print "\t参数: %s" % params
    res = solr.search(q, **params)
    print "召回数: %s" % len(res.docs)
    return len(res.docs)


def test_of_all_price_filter():
    # all_price_filter(1, [100, 200])
    # min_price_filter(1, [100, 200])
    min_all_price_filter(1, [100, 200])


def save_price_filter(file_name):
    print "\t文件名: %s" % file_name
    workbook = xlsxwriter.Workbook(file_name)
    worksheet = workbook.add_worksheet()
    worksheet.write_row(0, 0, [u'筛选条件', u'所有价格', u'最低价格', u'组合价格'])
    count = 1

    clinic_list = FIRST_CLINICS
    filter_list = [['0', '10'], ['11', '30'], ['31', '50'], ['51', '*']]

    for clinic_no in clinic_list:
        for filter_price in filter_list:
            all_num = all_price_filter(clinic_no, filter_price)
            min_num = min_price_filter(clinic_no, filter_price)
            com_num = min_all_price_filter(clinic_no, filter_price)

            clinic_name = ClinicUtils.clinic_no_to_name(clinic_no)
            price_str = "-".join(filter_price)
            title_line = "%s, %s" % (clinic_name, price_str)
            data_line = [title_line, str(all_num), str(min_num), str(com_num)]
            worksheet.write_row(count, 0, data_line)
            count += 1
            if count % 200 == 0:
                print "count %s" % count

    workbook.close()
    print "\t写入文件完成! 行数: %s" % count


def test_save_price_filter():
    file_name = os.path.join(ROOT_DIR, PRICE_FILTER_DATA, "price_filter_%s.xlsx" % get_current_time_str())
    save_price_filter(file_name)


if __name__ == '__main__':
    # test_of_all_price_filter()
    test_save_price_filter()
