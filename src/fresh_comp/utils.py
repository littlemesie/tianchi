#! /usr/bin/python3
# coding=utf-8
import os
import random
import pickle

base_path ="/Volumes/d/fresh_comp_offline/"
# base_path = os.path.dirname(os.path.abspath(__file__)) + "/../../data/fresh_comp/"

def save_file(filepath, data):
    """
        保存数据
        @param filepath:    保存路径
        @param data:    要保存的数据
    """
    parent_path = filepath[: filepath.rfind("/")]

    if not os.path.exists(parent_path):
        os.mkdir(parent_path)
    with open(filepath, "wb") as f:
        pickle.dump(data, f)


def load_file(filepath):
    """载入二进制数据"""
    with open(filepath, "rb") as f:
        data = pickle.load(f)
    return data

def loadfile(filename):
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


def read_train_user_data(path=base_path + "tianchi_fresh_comp_train_user.csv", train_rate=1., seed=1):

    """
    载入train_user
    @param path:  文件路径
    @param train_rate:   训练集所占整个数据集的比例，默认为1，表示所有的返回数据都是训练集
    @return: (训练集，测试集)
    """
    trainset = list()
    testset = list()
    random.seed(seed)
    for line in loadfile(filename=path):
        arr_list = line.split(',')
        user = arr_list[0]
        item = arr_list[1]
        if random.random() < train_rate:
            trainset.append([int(user), int(item)])
        else:
            testset.append([int(user), int(item)])
    return trainset, testset

def read_train_item_data(path=base_path + "tianchi_fresh_comp_train_item.csv"):
    """载入train_item"""
    for line in loadfile(filename=path):
        arr_list = line.split(',')

if __name__ == '__main__':
    trainset, testset = read_train_user_data()
    print(trainset)