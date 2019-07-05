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


def read_rating_data(path=base_path + "train_test.csv", train_rate=1., seed=1):

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

if __name__ == '__main__':
    # trainset, testset = read_rating_data()
    # print(trainset)
    item_load_data()