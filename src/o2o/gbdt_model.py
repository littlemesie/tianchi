# -*- coding:utf-8 -*-
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import auc, roc_curve, accuracy_score, roc_auc_score
from o2o.feature.extract_feature01 import process_data
from o2o.feature.data_util import load_data,split_data, load_test
from src.utils.model_util import load_model, save_model

def train(train_data, valid_data):
    """训练模型"""
    original_feature = ['discount_rate', 'discount_type', 'discount_man', 'discount_jian', 'distance', 'weekday',
                        'weekday_type']
    train_X = train_data[original_feature]
    train_y = train_data['label']
    valid_X = valid_data[original_feature]
    valid_y = np.array(valid_data['label'])

    gbdt = GradientBoostingClassifier()
    model = gbdt.fit(train_X, train_y)
    y_predprob = gbdt.predict_proba(valid_X)
    prob = np.array(y_predprob[:,1])
    print(prob)
    fpr, tpr, thresholds = roc_curve(valid_y, prob)
    aucs = auc(fpr, tpr)
    print(aucs)
    # print("AUC Score (Train): %f" % roc_auc_score(valid_y, y_predprob))


if __name__ == '__main__':
    pd_train, pd_test = load_data()
    pd_train = process_data(pd_train, lable=True)
    train_data, valid_data = split_data(pd_train)
    train(train_data, valid_data)
    # pd_test = process_data(pd_test)
    # train_data, valid_data = split_data(pd_train)
    # model = train(train_data, valid_data)