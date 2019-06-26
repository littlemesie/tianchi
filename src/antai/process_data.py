#! /usr/bin/python3
# coding=utf-8
import os
import random

base_path = "/Volumes/d/antai/"

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
    for line in loadfile(filename=path):
        arr = line.split(',')
        if random.random() < train_rate:
            trainset.append([int(arr[1]), int(arr[2]), int(1)])
        else:
            testset.append([int(arr[1]), int(arr[2]), int(1)])
    return trainset, testset

if __name__ == '__main__':
    trainset, testset = read_rating_data()
    print(trainset)