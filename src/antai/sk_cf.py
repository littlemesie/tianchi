#!/usr/bin/python
# coding:utf8

import math
from operator import itemgetter

import numpy as np
import pandas as pd
import dask.array as da
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.metrics.pairwise import pairwise_distances
from antai.utils import load_file, save_file


def split_data(data_file="/Volumes/d/antai/train_test.csv", test_size=0.2):
    # 加载数据集
    # header = ['buyer_country_id', 'buyer_admin_id', 'item_id', 'create_order_time', 'irank']
    data = pd.read_csv(data_file)
    df = data[['buyer_admin_id', 'item_id']]
    n_users = df.buyer_admin_id.unique()
    n_items = df.item_id.unique()
    users = dict(zip(n_users, range(len(n_users))))
    items = dict(zip(n_items, range(len(n_items))))
    train_data, test_data = train_test_split(df, test_size=test_size)

    return users, items, train_data, test_data


def calc_similarity(users, items, train_data, test_data):
    # 创建用户产品矩阵，针对测试数据和训练数据，创建两个矩阵：
    print(len(users), len(items))
    train_data_matrix = da.zeros((len(users), len(items)))
    # train_data_matrix = np.zeros((len(users), len(items)))

    for line in train_data.itertuples():
        user_index = users.get(line[0])
        item_index = items.get(line[1])
        train_data_matrix[user_index, item_index] = 1
    # test_data_matrix = np.zeros((len(users), len(items)))
    # for line in test_data.itertuples():
    #     test_data_matrix[line[1], line[2]] = 1

    # 使用sklearn的pairwise_distances函数来计算余弦相似性。
    sim_matrix_path = 'store/user_sim.pkl'
    print(sim_matrix_path)
    # try:
    #     user_similarity = load_file(sim_matrix_path)
    # except Exception:
    #     user_similarity = pairwise_distances(train_data_matrix, metric="cosine")
    #     save_file('store/user_sim.pkl', user_similarity)

    return train_data_matrix, test_data_matrix, user_similarity


def predict(rating, similarity, type='user'):
    print(type)
    print("rating=", np.shape(rating))
    print("similarity=", np.shape(similarity))

    if type == 'item':
        # 综合打分： 人-电影-评分(943, 1682)*电影-电影-距离(1682, 1682)=结果-人-电影(各个电影对同一电影的综合得分)(943, 1682)  ／  再除以  电影与其他电影总的距离 = 人-电影综合得分
        pred = rating.dot(similarity) / np.array([np.abs(similarity).sum(axis=1)])
    else:
        # 求出每一个用户，所有电影的综合评分（axis=0 表示对列操作， 1表示对行操作）
        mean_user_rating = rating.mean(axis=1)
        rating_diff = (rating - mean_user_rating[:, np.newaxis])

        # 均分  +  人-人-距离(943, 943)*人-电影-评分diff(943, 1682)=结果-人-电影（每个人对同一电影的综合得分）(943, 1682)  再除以  个人与其他人总的距离 = 人-电影综合得分
        pred = mean_user_rating[:, np.newaxis] + similarity.dot(rating_diff) / np.array([np.abs(similarity).sum(axis=1)]).T
    return pred


def rmse(prediction, ground_truth):
    prediction = prediction[ground_truth.nonzero()].flatten()
    ground_truth = ground_truth[ground_truth.nonzero()].flatten()
    return math.sqrt(mean_squared_error(prediction, ground_truth))


def evaluate(prediction, item_popular, name):
    hit = 0
    rec_count = 0
    test_count = 0
    popular_sum = 0
    all_rec_items = set()
    for u_index in range(len(users)):
        items = np.where(train_data_matrix[u_index, :] == 0)[0]
        pre_items = sorted(
            dict(zip(items, prediction[u_index, items])).items(),
            key=itemgetter(1),
            reverse=True)[:20]
        test_items = np.where(test_data_matrix[u_index, :] != 0)[0]

        # 对比测试集和推荐集的差异 item, w
        for item, _ in pre_items:
            if item in test_items:
                hit += 1
            all_rec_items.add(item)

            # 计算用户对应的电影出现次数log值的sum加和
            if item in item_popular:
                popular_sum += math.log(1 + item_popular[item])

        rec_count += len(pre_items)
        test_count += len(test_items)

    precision = hit / (1.0 * rec_count)
    recall = hit / (1.0 * test_count)
    coverage = len(all_rec_items) / (1.0 * len(item_popular))
    popularity = popular_sum / (1.0 * rec_count)
    print('%s: precision=%.4f \t recall=%.4f \t coverage=%.4f \t popularity=%.4f' % (name, precision, recall, coverage, popularity))


def recommend(u_index, prediction):
    items = np.where(train_data_matrix[u_index, :] == 0)[0]
    pre_items = sorted(
        dict(zip(items, prediction[u_index, items])).items(),
        key=itemgetter(1),
        reverse=True)[:10]
    test_items = np.where(test_data_matrix[u_index, :] != 0)[0]

    print('原始结果：', test_items)
    print('推荐结果：', [key for key, value in pre_items])


if __name__ == "__main__":

    # users, items, train_data, test_data = split_data()
    #
    # # 计算相似度
    # train_data_matrix, test_data_matrix, user_similarity = calc_similarity(
    #     users, items, train_data, test_data)
    #
    # user_prediction = predict(train_data_matrix, user_similarity, type='user')
    #
    # # 评估：均方根误差
    # print('User based CF RMSE: ' + str(rmse(user_prediction, test_data_matrix)))
    #
    # # # 推荐结果
    # recommend(1, user_prediction)
    import dask.array as da
    arr = da.random.random(size=(1000000, 1000000),
                         chunks=(10000, 10000))
    arr[0][0] = 1
    print(arr[0][0])
    # for a in arr:
    #     for x in a:
    #         print(x.A)
    #         break
    #     break

