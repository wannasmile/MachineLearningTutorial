# -*- coding:utf-8 -*-
from __future__ import unicode_literals

import os
import sys
from datetime import datetime

p = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if p not in sys.path:
    sys.path.append(p)


class SearchDoctorsObject():
    """
    搜索医生的信息统计
    """
    data_type = type(str)  # 推荐组的文字
    pos_of_list = type(str)  # 位置信息: 1, 2, 3, o
    f_clinic_no = type(str)  # 一级科室
    s_clinic_no = type(str)  # 二级科室
    data_source = type(str)  # 数据来源: cc(click_clinic), se(search), fi(filter), sort(so)

    search_pv = type(int)
    search_uv = type(int)
    search_sum = type(int)

    click_pv = type(int)
    click_uv = type(int)
    click_sum = type(int)

    purchase_pv = type(int)
    purchase_uv = type(int)
    purchase_sum = type(int)

    record_time = type(str)
    last_modified = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 最新修改

    def info(self):
        print "data_type:%s, pos_of_list:%s, f_clinic_no:%s, s_clinic_no:%s, data_source:%s, " \
              "search_pv:%s, search_uv:%s, search_sum:%s,  " \
              "click_pv:%s, click_uv:%s, click_sum:%s, " \
              "purchase_pv:%s, purchase_uv:%s, purchase_sum:%s, " \
              "record_time:%s, last_modified:%s" % \
              (self.data_type, self.pos_of_list, self.f_clinic_no, self.s_clinic_no, self.data_source,
               self.search_pv, self.search_uv, self.search_sum,
               self.click_pv, self.click_uv, self.click_sum,
               self.purchase_pv, self.purchase_uv, self.purchase_sum,
               self.record_time, self.last_modified)
