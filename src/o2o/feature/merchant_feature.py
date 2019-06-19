# -*- coding:utf-8 -*-
import os
import pandas as pd
import numpy as np
from datetime import date
"""merchant feature:
    total_sales
    sales_use_coupon
    total_coupon
    coupon_rate = sales_use_coupon/total_sales.  
    transfer_rate = sales_use_coupon/total_coupon. 
    merchant_avg_distance
    merchant_min_distance
    merchant_max_distance
"""

def get_merchant_feature(dataset, base_path, feature_name):
    """"""

    t = dataset[['merchant_id']]
    t.drop_duplicates(inplace=True)

    t1 = dataset[dataset.date != 'null'][['merchant_id']]
    t1['total_sales'] = 1
    t1 = t1.groupby('merchant_id').agg('sum').reset_index()

    t2 = dataset[(dataset.date != 'null') & (dataset.coupon_id != 'null')][['merchant_id']]
    t2['sales_use_coupon'] = 1
    t2 = t2.groupby('merchant_id').agg('sum').reset_index()

    t3 = dataset[dataset.coupon_id != 'null'][['merchant_id']]
    t3['total_coupon'] = 1
    t3 = t3.groupby('merchant_id').agg('sum').reset_index()

    t4 = dataset[(dataset.date != 'null') & (dataset.coupon_id != 'null')][['merchant_id', 'distance']]
    t4.replace('null', -1, inplace=True)
    t4.distance = t4.distance.astype('int')
    t4.replace(-1, np.nan, inplace=True)
    t5 = t4.groupby('merchant_id').agg('min').reset_index()
    t5.rename(columns={'distance': 'merchant_min_distance'}, inplace=True)

    t6 = t4.groupby('merchant_id').agg('max').reset_index()
    t6.rename(columns={'distance': 'merchant_max_distance'}, inplace=True)

    t7 = t4.groupby('merchant_id').agg('mean').reset_index()
    t7.rename(columns={'distance': 'merchant_mean_distance'}, inplace=True)

    t8 = t4.groupby('merchant_id').agg('median').reset_index()
    t8.rename(columns={'distance': 'merchant_median_distance'}, inplace=True)

    merchant_feature = pd.merge(t, t1, on='merchant_id', how='left')
    merchant_feature = pd.merge(merchant_feature, t2, on='merchant_id', how='left')
    merchant_feature = pd.merge(merchant_feature, t3, on='merchant_id', how='left')
    merchant_feature = pd.merge(merchant_feature, t5, on='merchant_id', how='left')
    merchant_feature = pd.merge(merchant_feature, t6, on='merchant_id', how='left')
    merchant_feature = pd.merge(merchant_feature, t7, on='merchant_id', how='left')
    merchant_feature = pd.merge(merchant_feature, t8, on='merchant_id', how='left')
    merchant_feature.sales_use_coupon = merchant_feature.sales_use_coupon.replace(np.nan, 0)  # fillna with 0
    merchant_feature['merchant_coupon_transfer_rate'] = merchant_feature.sales_use_coupon.astype(
        'float') / merchant_feature.total_coupon
    merchant_feature['coupon_rate'] = merchant_feature.sales_use_coupon.astype(
        'float') / merchant_feature.total_sales
    merchant_feature.total_coupon = merchant_feature.total_coupon.replace(np.nan, 0)
    merchant_feature.to_csv(base_path + feature_name, index=None)

if __name__ == '__main__':
    base_path = os.path.dirname(os.path.abspath(__file__)) + "/../../../data/o2o/"
    test = pd.read_csv(base_path + 'test.csv', keep_default_na=False)
    test.columns = ['user_id', 'merchant_id', 'coupon_id', 'discount_rate', 'distance', 'date_received', 'date']
    dataset = test[test.date_received != 'null']
    feature_name = "data/test_merchant_feature.csv"
    coupon_feature = get_merchant_feature(dataset, base_path, feature_name)