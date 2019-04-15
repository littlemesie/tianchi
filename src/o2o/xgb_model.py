# -*- coding:utf-8 -*-

"""
@ide: PyCharm
@author: mesie
@date: 2019/4/14 20:14
@summary:
"""

import numpy as np
import xgboost as xgb
from sklearn.metrics import auc, roc_curve
from o2o.feature.extract_feature01 import process_data
from o2o.feature.data_util import load_data,split_data


def train(train_data, valid_data):
    original_feature = ['discount_rate', 'discount_type', 'discount_man', 'discount_jian', 'distance', 'weekday',
                        'weekday_type']
    dtrain = xgb.DMatrix(train_data[original_feature], train_data['label'])
    dtest = xgb.DMatrix(valid_data[original_feature])
    test_label = np.array(valid_data['label'])
    params = {'booster': 'gbtree',
              'objective': 'rank:pairwise',
              'eval_metric': 'auc',
              'gamma': 0.1,
              'min_child_weight': 1.1,
              'max_depth': 5,
              'lambda': 10,
              'subsample': 0.7,
              'colsample_bytree': 0.7,
              'colsample_bylevel': 0.7,
              'eta': 0.01,
              'tree_method': 'exact',
              'seed': 0,
              'nthread': 12
              }

    watchlist = [(dtrain, 'train')]
    model = xgb.train(params, dtrain, num_boost_round=100, evals=watchlist)
    prob = np.array(model.predict(dtest))
    fpr, tpr, thresholds = roc_curve(test_label, prob)
    aucs = auc(fpr, tpr)
    print(aucs)


if __name__ == '__main__':
    pd_train, pd_test = load_data()
    pd_train = process_data(pd_train, lable=True)
    # pd_test = process_data(pd_test)
    train_data, valid_data = split_data(pd_train)
    train(train_data, valid_data)
