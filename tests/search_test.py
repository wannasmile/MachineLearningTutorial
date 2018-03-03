#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试搜索

Created by C.L.Wang
"""
import json

from remote_access.services.remote_services import get_search_service


def test_search():
    service = get_search_service()
    query_param = {'start': '0', 'rows': '10'}
    filter_param = {'clinic_no': '1', 'zm_hospital': 1, 'is_tel_price_v2': 1}
    raw_json = {'query': query_param, 'filter': filter_param}
    res = service.search_doctors_for_debug(json.dumps(raw_json))
    print res


if __name__ == '__main__':
    test_search()
