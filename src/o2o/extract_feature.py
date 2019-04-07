# -*- coding:utf-8 -*-

"""
@ide: PyCharm
@author: mesie
@date: 2019/4/7 21:25
@summary: 提取特征
https://blog.csdn.net/red_stone1/article/details/83859845
"""
import os, sys, pickle
import numpy as np
import pandas as pd
from datetime import date

base_path = os.path.dirname(os.path.abspath(__file__)) + "/../../data/o2o/"

def load_data():
    pd_train = pd.read_csv(base_path + 'ccf_offline_stage1_train.csv', keep_default_na=False)
    pd_test = pd.read_csv(base_path + 'ccf_offline_stage1_train.csv', keep_default_na=False)
    # print('有优惠卷，购买商品：%d' % pd_train[(pd_train['Date_received'] != 'null') & (pd_train['Date'] != 'null')].shape[0])
    # print('有优惠卷，未购商品：%d' % pd_train[(pd_train['Date_received'] != 'null') & (pd_train['Date'] == 'null')].shape[0])
    # print('无优惠卷，购买商品：%d' % pd_train[(pd_train['Date_received'] == 'null') & (pd_train['Date'] != 'null')].shape[0])
    # print('无优惠卷，未购商品：%d' % pd_train[(pd_train['Date_received'] == 'null') & (pd_train['Date'] == 'null')].shape[0])
    # print('Discount_rate 类型：\n', pd_train['Discount_rate'].unique())
    return pd_train, pd_test


def get_discount_type(row):
    """打折类型"""
    if row == 'null':
        return 'null'
    elif ':' in row:
        return 1
    else:
        return 0


def convert_rate(row):
    """折扣率"""
    if row == 'null':
        return 1.0
    elif ':' in row:
        rows = row.split(':')
        return 1.0 - float(rows[1]) / float(rows[0])
    else:
        return float(row)


def get_discount_man(row):
    """满多少"""
    if ':' in row:
        rows = row.split(':')
        return int(rows[0])
    else:
        return 0


def get_discount_jian(row):
    """减多少"""
    if ':' in row:
        rows = row.split(':')
        return int(rows[1])
    else:
        return 0


def process_data(df):
    df['discount_type'] = df['Discount_rate'].apply(get_discount_type)
    df['discount_rate'] = df['Discount_rate'].apply(convert_rate)
    df['discount_man'] = df['Discount_rate'].apply(get_discount_man)
    df['discount_jian'] = df['Discount_rate'].apply(get_discount_jian)

    # print(df['discount_rate'].unique())

    return df


if __name__ == '__main__':
    pd_train, pd_test = load_data()
    d = process_data(pd_train)
    print(d.head(5))