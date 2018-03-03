#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
细分儿科

Created by C.L.Wang
"""

import pyexcel
import pytrie
import xlsxwriter

from project_utils import *
from rcm_doctors.constants import TRIAGE_ERKE_DATA
from root_dir import ROOT_DIR


def process_erke_triage(rf_name, wf_name, clinic_folder):
    """
    加载Excel文件
    :param rf_name 读取文件
    :param wf_name 写入文件
    :param clinic_folder 科室文件夹
    :return: 
    """
    print "\t读取文件: %s" % rf_name
    print "\t写入文件: %s" % wf_name
    print "\t科室文件夹: %s" % clinic_folder
    create_file(wf_name)

    excel_data = pyexcel.load(read_file)  # 读取文件
    lines = excel_data.get_internal_array()

    clinic_words_trie = load_clinic_words_trie(clinic_folder)  # 处理科室文件夹

    workbook = xlsxwriter.Workbook(wf_name)  # 创建Excel
    worksheet = workbook.add_worksheet()  # 添加Sheet

    worksheet.write_row(0, 0, lines[1] + [u'第一细分', u'词数', u'匹配词', u'第二细分', u'词数', u'匹配词'])
    count = 1

    for index in range(2, len(lines)):
        problem_info = unicode(lines[index][1])
        data_line = lines[index] + parse_problem_info(problem_info, clinic_words_trie)
        worksheet.write_row(count, 0, data_line)
        count += 1
        if count % 10000 == 0:
            print "\t count %s" % count
    print "写入行数: %s" % count


def load_clinic_words_trie(clinic_folder):
    keyword_to_clinics_dict = {}
    paths_list, names_list = listdir_files(clinic_folder)
    for index in range(len(paths_list)):
        data_lines = read_file(paths_list[index])
        clinic_name = unicode(names_list[index])
        for data_line in data_lines:
            keyword = unicode(data_line.strip().lower())
            keyword_to_clinics_dict.setdefault(keyword, set())
            keyword_to_clinics_dict[keyword].add(clinic_name)
    return pytrie.SortedStringTrie(**keyword_to_clinics_dict)


def parse_problem_info(problem_info, clinic_words_trie):
    problem_info = problem_info.lower()
    # 用来存储匹配后的 关键字 与 对应科室的 key－value
    # {keyword: set(['ac', 'a']),  keyword:set(['1', 'b']), }
    keywords_clinics_dict = {}
    # trie_clinic_principles是加载了各个科室关键字的前缀树
    current_position, content_length = 0, len(problem_info)
    while current_position < content_length:
        for keyword, set_clinics in list(clinic_words_trie.iter_prefix_items(problem_info[current_position:])):
            # 如果匹配成功 应当返回形式：[(u"keyword1", set([1])),  (u"keyword2", set([1,3])), ... ]
            # 每个element 形式为 (u"keyword", set([1]))
            keywords_clinics_dict[keyword] = set_clinics
        current_position += 1

    clinics_dict, clinic_keys_dict = keyword_filter(keywords_clinics_dict)
    # for clinic in clinic_keys_dict.keys():
    #     print "\t%s: %s" % (clinic, list_2_utf8(list(clinic_keys_dict[clinic])))
    div_data_list = parse_clinics_dict(clinics_dict, clinic_keys_dict)
    return div_data_list


def parse_clinics_dict(clinics_dict, clinic_keys_dict):
    clinics_num_list = sort_dict_by_value(clinics_dict)
    try:
        df_clinic = clinics_num_list[0][0]
        df_num = clinics_num_list[0][1]
        df_list = list_2_utf8(list(clinic_keys_dict[df_clinic]))
    except:
        df_clinic = ""
        df_num = ""
        df_list = ""
    try:
        ds_clinic = clinics_num_list[1][0]
        ds_num = clinics_num_list[1][1]
        ds_list = list_2_utf8(list(clinic_keys_dict[ds_clinic]))
    except:
        ds_clinic = ""
        ds_num = ""
        ds_list = ""
    return [df_clinic, df_num, df_list, ds_clinic, ds_num, ds_list]


def keyword_filter(keyword_dict):
    # 用来存储去除关键词之间 包含 与 被包含  关系后的剩余keys
    keys_dict = {}
    clinic_keys_dict = {}

    if keyword_dict:
        keyword_list = [key for key in keyword_dict.keys()]
        for index_first in range(0, (len(keyword_list) - 1)):
            for index_second in range((index_first + 1), len(keyword_list)):
                if keyword_list[index_first] in keyword_list[index_second]:
                    keyword_dict[keyword_list[index_first]] = None
                elif keyword_list[index_second] in keyword_list[index_first]:
                    keyword_dict[keyword_list[index_second]] = None
                else:
                    pass
        # 包含与被包含关系已整理完毕， 需要对传入的dict进行封装 并返回最终的封装结果
        # 返回的结果是一个 list
        keys_dict = collections.defaultdict(int)

        for key, value in keyword_dict.items():
            if value:
                for item in value:
                    keys_dict[item] += 1
                    item = unicode(item)
                    if item not in clinic_keys_dict:
                        clinic_keys_dict[item] = set()
                    clinic_keys_dict[item].add(unicode(key))

    return keys_dict, clinic_keys_dict


def test_of_process_erke_triage():
    rf_name = os.path.join(ROOT_DIR, TRIAGE_ERKE_DATA, 'export_paediatrics_problems_0209.xlsx')
    wf_name = os.path.join(ROOT_DIR, TRIAGE_ERKE_DATA, 'result_of_erke_triage_%s.xlsx' % get_current_time_str())
    clinic_folder = os.path.join(ROOT_DIR, TRIAGE_ERKE_DATA, 'clinics')
    process_erke_triage(rf_name, wf_name, clinic_folder)


if __name__ == '__main__':
    test_of_process_erke_triage()
