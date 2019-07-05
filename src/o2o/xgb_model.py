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
from o2o.feature.data_util import load_data,split_data, load_test
from src.utils.model_util import load_model, save_model


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
    model_path = "model/xgb.pkl"
    try:
        model = load_model(model_path)
    except:
        pass
        model = xgb.train(params, dtrain, num_boost_round=100, evals=watchlist)
        save_model(model_path, model)
    prob = np.array(model.predict(dtest))
    fpr, tpr, thresholds = roc_curve(test_label, prob)
    aucs = auc(fpr, tpr)
    print(aucs)
    return model

def test(model, test_data):
    """训练数据集"""

    original_feature = ['discount_rate', 'discount_type', 'discount_man', 'discount_jian', 'distance', 'weekday',
                        'weekday_type']
    dtest = xgb.DMatrix(test_data[original_feature])
    prob = np.array(model.predict(dtest))
    print(prob)

if __name__ == '__main__':
    # pd_train, pd_test = load_data()
    # pd_train = process_data(pd_train, lable=True)
    # pd_test = process_data(pd_test)
    # train_data, valid_data = split_data(pd_train)
    # model = train(train_data, valid_data)
    import time
    t1 = time.time()*1000
    pd_test = load_test()
    pd_test = process_data(pd_test)
    model_path = "model/xgb.pkl"
    model = load_model(model_path)
    test(model, pd_test)
    t2 = time.time()*1000
    print(t2-t1)

