# -*- coding:utf-8 -*-
import os
import pandas as pd
import numpy as np
from datetime import date
"""coupon feature:
    discount_rate
    discount_man
    discount_jian
    is_man_jian
    day_of_week
    day_of_month. (date_received)
"""

def calc_discount_rate(s):
    s = str(s)
    s = s.split(':')
    if len(s) == 1:
        return float(s[0])
    else:
        return 1.0 - float(s[1]) / float(s[0])


def get_discount_man(s):
    s = str(s)
    s = s.split(':')
    if len(s) == 1:
        return 'null'
    else:
        return int(s[0])


def get_discount_jian(s):
    s = str(s)
    s = s.split(':')
    if len(s) == 1:
        return 'null'
    else:
        return int(s[1])


def is_man_jian(s):
    s = str(s)
    s = s.split(':')
    if len(s) == 1:
        return 0
    else:
        return 1

def get_coupon_feature(dataset, base_path, feature_name):
    """"""
    dataset['day_of_week'] = dataset.date_received.astype('str').apply(
        lambda x: date(int(x[0:4]), int(x[4:6]), int(x[6:8])).weekday() + 1)
    dataset['day_of_month'] = dataset.date_received.astype('str').apply(lambda x: int(x[6:8]))
    dataset['days_distance'] = dataset.date_received.astype('str').apply(
        lambda x: (date(int(x[0:4]), int(x[4:6]), int(x[6:8])) - date(2016, 6, 30)).days)
    dataset['discount_man'] = dataset.discount_rate.apply(get_discount_man)
    dataset['discount_jian'] = dataset.discount_rate.apply(get_discount_jian)
    dataset['is_man_jian'] = dataset.discount_rate.apply(is_man_jian)
    dataset['discount_rate'] = dataset.discount_rate.apply(calc_discount_rate)
    d = dataset[['coupon_id']]
    d['coupon_count'] = 1
    d = d.groupby('coupon_id').agg('sum').reset_index()
    coupon_feature = pd.merge(dataset, d, on='coupon_id', how='left')
    coupon_feature.to_csv(base_path + feature_name, index=None)
    return coupon_feature

if __name__ == '__main__':
    base_path = os.path.dirname(os.path.abspath(__file__)) + "/../../../data/o2o/"
    test = pd.read_csv(base_path + 'test.csv', keep_default_na=False)
    test.columns = ['user_id', 'merchant_id', 'coupon_id', 'discount_rate', 'distance', 'date_received', 'date']
    dataset = test[test.date_received != 'null']
    feature_name = "data/test_coupon_feature.csv"
    coupon_feature = get_coupon_feature(dataset, base_path, feature_name)