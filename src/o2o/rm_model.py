import numpy as np

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import auc, roc_curve
from o2o.feature.extract_feature01 import process_data
from o2o.feature.data_util import load_data,split_data

def train(train_data, valid_data):
    """"""
    random_forest = RandomForestClassifier(n_estimators=100)
    original_feature = ['discount_rate', 'discount_type', 'discount_man', 'discount_jian', 'distance', 'weekday',
                        'weekday_type']
    random_forest.fit(train_data[original_feature], train_data['label'])
    y_valid_pred = random_forest.predict_proba(valid_data[original_feature])
    valid = valid_data.copy()
    valid['pred_prob'] = y_valid_pred[:, 1]
    print(y_valid_pred)
    calculation_auc(valid)

def calculation_auc(valid):
    vg = valid.groupby(['Coupon_id'])
    aucs = []
    for i in vg:
        tmpdf = i[1]
        if len(tmpdf['label'].unique()) != 2:
            continue
        fpr, tpr, thresholds = roc_curve(tmpdf['label'], tmpdf['pred_prob'], pos_label=1)
        aucs.append(auc(fpr, tpr))
    print(np.average(aucs))

if __name__ == '__main__':
    pd_train, pd_test = load_data()
    pd_train = process_data(pd_train, lable=True)
    # pd_test = process_data(pd_test)
    train_data, valid_data = split_data(pd_train)
    train(train_data, valid_data)