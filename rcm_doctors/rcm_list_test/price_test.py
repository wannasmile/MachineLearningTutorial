#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试价格：按上周的数据，高价组展示单价67左右，低价组展示单价23，普通付费组定40左右就行

Created by C.L.Wang
"""
import math


def price_value(x):
    return math.log(x + 1)


if __name__ == '__main__':
    print price_value(67) / 67.0
    print 1 / 23.0
    print 0.55 * price_value(40) / 40.0
    print price_value(40.0 / 56.0)
    print price_value(40.0 / 32.0)
