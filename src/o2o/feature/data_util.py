# -*- coding:utf-8 -*-

import os
import numpy as np
import pandas as pd
from datetime import date

base_path = os.path.dirname(os.path.abspath(__file__)) + "/../../data/o2o/"

def load_data():
    """打开数据集"""
    pd_train = pd.read_csv(base_path + 'ccf_offline_stage1_train.csv', keep_default_na=False)
    pd_test = pd.read_csv(base_path + 'ccf_offline_stage1_test_revised.csv', keep_default_na=False)

    # print('有优惠卷，购买商品：%d' % pd_train[(pd_train['Coupon_id'] != 'null') & (pd_train['Date'] != 'null')].shape[0])
    # print('有优惠卷，未购商品：%d' % pd_train[(pd_train['Coupon_id'] != 'null') & (pd_train['Date'] == 'null')].shape[0])
    # print('无优惠卷，购买商品：%d' % pd_train[(pd_train['Coupon_id'] == 'null') & (pd_train['Date'] != 'null')].shape[0])
    # print('无优惠卷，未购商品：%d' % pd_train[(pd_train['Coupon_id'] == 'null') & (pd_train['Date'] == 'null')].shape[0])
    # print('Discount_rate 类型：\n', pd_train['Discount_rate'].unique())
    # print('Distance 类型：\n', pd_train['Distance'].unique())
    return pd_train, pd_test

def split_data(df):
    """数据集划分"""
    df = df[df['label'] != -1].copy()
    train_data = df[(df['Date_received'] < '20160516')].copy()
    valid_data = df[(df['Date_received'] >= '20160516') & (df['Date_received'] <= '20160615')].copy()
    print('Train Set: \n', train_data['label'].value_counts())
    print('Valid Set: \n', valid_data['label'].value_counts())
    return train_data, valid_data