# -*- coding:utf-8 -*-

import numpy as np
import pandas as pd
import xgboost as xgb
from src.utils.model_util import save_model, load_model
base_path = "/Volumes/d/antai/"

def train():
    feature = ['cate_id', 'store_id', 'item_price', 'sunday', 'monday', 'tuesday', 'wednesday',
                        'thursday', 'friday', 'saturday']
    train_data = pd.read_csv(base_path + 'items_feature.csv')
    dtrain = xgb.DMatrix(train_data[feature], label=train_data['buy_cnt'])
    test = xgb.DMatrix(train_data[feature])
    params = {'booster': 'gbtree',
              'objective': 'rank:pairwise',
              'eval_metric': 'logloss',
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
    model_path = "store/xgb.pkl"
    # model = xgb.train(params, dtrain, num_boost_round=19, evals=watchlist)
    # save_model(model_path, model)
    model = load_model(model_path)
    prob = np.array(model.predict(test))
    result = pd.DataFrame(train_data['item_id'])
    result['prob'] = prob
    result.to_csv(base_path + 'result.csv', index=False)
    print(result)

# if __name__ == '__main__':
#     train()

