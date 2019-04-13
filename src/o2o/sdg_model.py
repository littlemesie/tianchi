import numpy as np

from sklearn.model_selection import StratifiedKFold, GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import auc, roc_curve
from o2o.feature.extract_feature01 import process_data
from o2o.feature.data_util import load_data,split_data


def check_model(data, predictors):
    classifier = lambda: SGDClassifier(
        loss='log',  # loss function: logistic regression
        penalty='elasticnet',  # L1 & L2
        fit_intercept=True,  # 是否存在截距，默认存在
        max_iter=100,
        shuffle=True,  # Whether or not the training data should be shuffled after each epoch
        n_jobs=1,  # The number of processors to use
        class_weight=None)  # Weights associated with classes. If not given, all classes are supposed to have weight one.

    # 管道机制使得参数集在新数据集（比如测试集）上的重复使用，管道机制实现了对全部步骤的流式化封装和管理。
    model = Pipeline(steps=[
        ('ss', StandardScaler()),  # transformer
        ('en', classifier())  # estimator
    ])

    parameters = {
        'en__alpha': [0.001, 0.01, 0.1],
        'en__l1_ratio': [0.001, 0.01, 0.1]
    }

    # StratifiedKFold用法类似Kfold，但是他是分层采样，确保训练集，测试集中各类别样本的比例与原始数据集中相同。
    folder = StratifiedKFold(n_splits=3, shuffle=True)

    # Exhaustive search over specified parameter values for an estimator.
    grid_search = GridSearchCV(
        model,
        parameters,
        cv=folder,
        n_jobs=-1,  # -1 means using all processors
        verbose=1)
    grid_search = grid_search.fit(data[predictors],
                                  data['label'])

    return grid_search

def train(train_data, valid_data, pd_test=None):
    original_feature = ['discount_rate', 'discount_type', 'discount_man', 'discount_jian', 'distance', 'weekday',
                        'weekday_type']
    model = check_model(train_data, original_feature)
    y_valid_pred = model.predict_proba(valid_data[original_feature])
    valid = valid_data.copy()
    valid['pred_prob'] = y_valid_pred[:, 1]
    calculation_auc(valid)
    # test(pd_test, model, original_feature)

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

def test(pd_test, model, original_feature):
    y_test_pred = model.predict_proba(pd_test[original_feature])
    dftest1 = pd_test[['User_id', 'Coupon_id', 'Date_received']].copy()
    dftest1['Probability'] = y_test_pred[:, 1]
    dftest1.to_csv('submit.csv', index= False, header=False)

if __name__ == '__main__':
    pd_train, pd_test = load_data()
    pd_train = process_data(pd_train, lable=True)
    # pd_test = process_data(pd_test)
    train_data, valid_data = split_data(pd_train)
    train(train_data, valid_data)
