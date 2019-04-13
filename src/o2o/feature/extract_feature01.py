# -*- coding:utf-8 -*-

"""
@ide: PyCharm
@author: mesie
@date: 2019/4/7 21:25
@summary: 简单提取特征
https://blog.csdn.net/red_stone1/article/details/83859845
"""
import pandas as pd
from datetime import date
from o2o.feature.data_util import load_data


def get_discount_type(row):
    """打折类型"""
    if row == 'null':
        return 0
    elif ':' in row:
        return 1
    else:
        return 2


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

def get_week_day(row):
    """日期处理"""

    if row == 'null':
        return row
    else:
        return date(int(row[0:4]), int(row[4:6]), int(row[6:8])).weekday() + 1

def get_label(row):
    """1:正样本，0负样本，-1表示没有领到优惠券，无需考虑 """
    if row['Date_received'] == 'null':
        return -1
    if row['Date'] != 'null':
        td = pd.to_datetime(row['Date'], format='%Y%m%d') - pd.to_datetime(row['Date_received'], format='%Y%m%d')
        if td <= pd.Timedelta(15, 'D'):
           return 1
    return 0


def process_data(df, lable=False):
    # 处理Discount_rate
    df['discount_type'] = df['Discount_rate'].apply(get_discount_type)
    df['discount_rate'] = df['Discount_rate'].apply(convert_rate)
    df['discount_man'] = df['Discount_rate'].apply(get_discount_man)
    df['discount_jian'] = df['Discount_rate'].apply(get_discount_jian)
    # 处理Distance
    df['distance'] = df['Distance'].replace('null', -1).astype(int)
    # 处理Date_received
    df['weekday'] = df['Date_received'].astype(str).apply(get_week_day)
    df['weekday'] = df['weekday'].replace('null', 0).astype(int)
    # weekday_type:周六和周日为1，其他为0
    df['weekday_type'] = df['weekday'].apply(lambda x: 1 if x in [6, 7] else 0)
    if lable:
        df['label'] = df.apply(get_label, axis=1)
    # print(df['discount_type'].unique())
    # print(df['label'].value_counts())
    # df = df[['discount_type'], ['discount_rate'], ['discount_man'], ['discount_jian'], ['distance'], ['weekday'],
    #         ['weekday_type'], ['label']]
    return df


if __name__ == '__main__':
    pd_train, pd_test = load_data()
    d = process_data(pd_train)
    print(d.head(5))