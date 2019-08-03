#! /usr/bin/python3
# coding=utf-8
import os
import random
import pandas as pd

base_path = "/Volumes/d/antai/"

def load_file(filename):
    """
    根据文件名载入数据
    :param filename:
    :return:
    """
    with open(filename, "r") as f:
        for i, line in enumerate(f):
            if i == 0:
                continue
            yield line


def read_rating_data(path, train_rate=1., seed=1):

    """
    载入评分数据
    @param path:  文件路径
    @param train_rate:   训练集所占整个数据集的比例，默认为1，表示所有的返回数据都是训练集
    @return: (训练集，测试集)
    """
    trainset = list()
    testset = list()
    random.seed(seed)
    for line in load_file(filename=path):
        arr = line.split(',')
        if arr[0] == 'yy':
            if random.random() < train_rate:
                trainset.append([int(arr[1]), int(arr[2]), int(1)])
            else:
                testset.append([int(arr[1]), int(arr[2]), int(1)])
    return trainset, testset

def item_load_data():
    """
    总数量:
        item:2416318
        cate_id:4119
        store_id:90783
        item_price:17932
    """
    item_data = pd.read_csv(base_path + "Antai_AE_round1_item_attr_20190625.csv", keep_default_na=False)
    item_id_count = item_data['item_id'].unique()
    cate_id_count = item_data['cate_id'].unique()
    store_id_count = item_data['store_id'].unique()
    item_price_count = item_data['item_price'].unique()
    print(len(item_id_count))
    print(len(cate_id_count))
    print(len(store_id_count))
    print(len(item_price_count))

def train_load_data():
    """
    总数量:
        buyer_admin_id:653593
        # xx buyer_admin_id:551246
        yy buyer_admin_id:102446
        yy item_id: 720556
    """
    trian_data = pd.read_csv(base_path + "Antai_AE_round1_train_20190625.csv", keep_default_na=False)
    temp = trian_data.loc[trian_data.buyer_country_id == 'yy']
    user_id_count = temp['buyer_admin_id'].unique()
    item_id_count = temp['item_id'].unique()
    print(len(user_id_count))
    print(len(item_id_count))

def test_load_data():
    test_data = pd.read_csv(base_path + "Antai_AE_round1_test_20190625.csv", keep_default_na=False)


def get_popular_items():
    """获取高频item"""
    item = pd.read_csv(base_path + 'Antai_AE_round1_item_attr_20190626.csv')
    # submit = pd.read_csv(path + 'Antai_AE_round1_submit_20190715.csv', header=None)
    # test = pd.read_csv(path + 'Antai_AE_round1_test_20190626.csv')
    train = pd.read_csv(base_path + 'Antai_AE_round1_train_20190626.csv')


    # 高频item_id
    temp = train.loc[train.buyer_country_id == 'yy']
    temp = temp.drop_duplicates(subset=['buyer_admin_id', 'item_id'], keep='first')
    item_cnts = temp.groupby(['item_id']).size().reset_index()
    item_cnts.columns = ['item_id', 'cnts']
    item_cnts = item_cnts.sort_values('cnts', ascending=False)
    items = item_cnts['item_id'].values.tolist()
    return items

if __name__ == '__main__':
    # trainset, testset = read_rating_data()
    # print(trainset)
    train_load_data()
