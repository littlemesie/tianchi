#! /usr/bin/python3
# coding=utf-8
import os
import time
import datetime
import random
import pandas as pd

base_path = "/Volumes/d/CIKM/round1_train/"

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

def user_behavior_statistics():
    """
    item总数量:
        pv:51987242
        buy:1958887
        cart:3381162
        fav:1424202
    不重复item总数量:
        pv:3750731
        buy:621971
        cart:1003676
        fav:628121
    """
    user_behavior = pd.read_csv(base_path + "user_behavior.csv", header=None, names=['user_id', 'item_id','behavior', 'time'])
    pv = user_behavior.loc[user_behavior.behavior == 'pv']
    buy = user_behavior.loc[user_behavior.behavior == 'buy']

    cart = user_behavior.loc[user_behavior.behavior == 'cart']
    fav = user_behavior.loc[user_behavior.behavior == 'fav']

    print(len(pv['item_id']))
    print(len(buy['item_id']))
    print(len(cart['item_id']))
    print(len(fav['item_id']))

def split_user_behavior():
    """"""
    user_behavior = pd.read_csv(base_path + "user_behavior.csv", header=None, names=['user_id', 'item_id', 'behavior', 'time'])
    pv = user_behavior.loc[user_behavior.behavior == 'pv']
    buy = user_behavior.loc[user_behavior.behavior == 'buy']

    cart = user_behavior.loc[user_behavior.behavior == 'cart']
    fav = user_behavior.loc[user_behavior.behavior == 'fav']

    pv.to_csv(base_path + 'pv.csv', index=False)
    buy.to_csv(base_path + 'buy.csv', index=False)
    cart.to_csv(base_path + 'cart.csv', index=False)
    fav.to_csv(base_path + 'fav.csv', index=False)

def test_load_data():
    """"""


def get_popular_items():
    """获取高频item"""
    buy_items = pd.read_csv(base_path + 'buy.csv')
    # 高频item_id
    temp = buy_items.drop_duplicates(subset=['user_id', 'item_id'], keep='first')
    item_cnts = temp.groupby(['item_id']).size().reset_index()
    item_cnts.columns = ['item_id', 'cnts']
    item_cnts = item_cnts.sort_values('cnts', ascending=False)
    items = item_cnts['item_id'].values.tolist()
    return items


def get_weekday(date):
    d = date[0:10]
    weekday = datetime.datetime.fromtimestamp(time.mktime(time.strptime(str(d), "%Y-%m-%d"))).weekday()
    return weekday

def get_item_attr():
    """处理商品属性"""
    train_data = pd.read_csv(base_path + 'Antai_AE_round1_train_20190626.csv')
    items_attr_data = pd.read_csv(base_path + 'Antai_AE_round1_item_attr_20190626.csv')
    result = pd.merge(train_data, items_attr_data, how='inner', on=['item_id', 'item_id'])
    temp = result.loc[result.buyer_country_id == 'yy']
    # temp =temp['item_id']
    temp['weekday'] = temp.create_order_time.apply(get_weekday)
    temp = temp[['item_id','weekday', 'cate_id','store_id','item_price']]
    temp.to_csv(base_path + 'yy_items_attr.csv', index=False, header=None)
    print(temp.head())


def get_weekday_list(data):
    temp = data.groupby(['weekday']).size().reset_index()
    temp.columns = ['weekday', 'cnts']
    weekday_list = [0, 0, 0, 0, 0, 0, 0]
    for index, row in temp.iterrows():
        weekday_list[row['weekday']] = row['cnts']
    return weekday_list

def get_item_feature():
    """"""
    train_data = pd.read_csv(base_path + 'yy_items_attr_1.csv')
    # test = train_data.loc[train_data.item_id == 6]
    # test = test.groupby(['weekday']).size().reset_index()
    # test.columns = ['weekday', 'cnts']
    # weekday_list = [0, 0, 0, 0, 0, 0, 0]
    # print(test)
    # for index, row in test.iterrows():
    #     weekday_list[row['weekday']] = row['cnts']
    #
    # print(weekday_list)

    items_feature = train_data.groupby(['item_id']).size().reset_index()
    items_feature.columns = ['item_id', 'cnts']
    train_data_2 = train_data.drop_duplicates(subset=['item_id'], keep='first')
    items_feature = pd.merge(items_feature, train_data_2, how='inner', on=['item_id', 'item_id'])
    with open(base_path + 'items_feature.csv','w') as f:
        f.write("lable,item_id,buy_cnt,cate_id,store_id,item_price,sunday,monday,tuesday,wednesday,thursday,friday,saturday" + '\n')
        for index, row in items_feature.iterrows():
            item_id = row['item_id']
            r = "1," + str(item_id) + ',' + str(row['cnts']) + ',' + str(row['cate_id']) + ',' + str(row['store_id']) + ',' \
                + str(row['item_price']) + ','
            data = train_data.loc[train_data.item_id == item_id]
            weekday_list = get_weekday_list(data)
            for i, w in enumerate(weekday_list):
                if i == 6:
                    r = r + str(w) + '\n'
                else:
                    r = r + str(w) + ','
            f.write(r)
        f.close()
    # items_feature = items_feature[[]]
    # print(items_feature)

if __name__ == '__main__':
    # user_behavior_statistics()
    split_user_behavior()
