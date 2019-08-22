# ! /usr/bin/python3
# coding=utf-8
import pandas as pd
from collections import defaultdict
import math
from operator import itemgetter
import sys
sys.path.extend(['/Users/t/python/tianchi', '/Users/t/python/tianchi/src'])
from utils.utils import load_file, save_file
from cikm.round2 import process_data



class UserCF(object):
    """用户协同过滤"""

    def __init__(self, train_path):
        self.train_data = self.init_train(train_path)

    def train(self, sim_matrix_path="store/buy_user_sim.pkl"):

        print("开始训练模型", file=sys.stderr)
        try:
            print("开始载入用户协同矩阵....", file=sys.stderr)
            self.user_sim_matrix = load_file(sim_matrix_path)
            print("载入协同过滤矩阵完成", file=sys.stderr)
        except BaseException:
            print("载入用户协同过滤矩阵失败，重新计算协同过滤矩阵", file=sys.stderr)
            # 计算用户协同矩阵
            self.user_sim_matrix = self.user_similarity()
            print("开始保存协同过滤矩阵", file=sys.stderr)
            save_file(sim_matrix_path, self.user_sim_matrix)
            print("保存协同过滤矩阵完成", file=sys.stderr)

    def init_train(self, train_path, data_path="store/train_data.pkl"):
        """初始化数据"""
        try:
            print("开始载入初始化数据....", file=sys.stderr)
            train_data = load_file(data_path)
        except BaseException:
            train_data = dict()
            for line in process_data.read_file(filename=train_path):
                arr = line.split('\t')
                train_data.setdefault(int(arr[0]), set())
                train_data[int(arr[0])].add(int(arr[1]))

            save_file(data_path, train_data)
        return train_data


    def user_similarity(self):
        """建立用户的协同过滤矩阵"""

        user_list = self.train_data.keys()
        for user in user_list:
            items = self.train_data.get(user)

        return
        # 建立用户倒排表
        item_user = dict()
        item_user_path = "store/item_user.pkl"
        try:
            print("开始载入用户倒排表....", file=sys.stderr)
            item_user = load_file(item_user_path)
        except BaseException:

            for user, items in self.train_data.items():
                print('user:' + str(user))
                for item in items:
                    item_user.setdefault(item, set())
                    item_user[item].add(user)
            save_file(item_user_path, item_user)

        # 建立用户协同过滤矩阵
        user_sim_matrix = dict()
        N = defaultdict(int)  # 记录用户购买商品数
        user_sim_matrix_path = "store/user_sim_matrix.pkl"
        N_path = "store/N.pkl"
        try:
            print("开始载入记录用户购买商品数....", file=sys.stderr)
            N = load_file(N_path)
        except Exception:
            for item, users in item_user.items():
                print('item:' + str(item))
                for u in users:
                    N[u] += 1
            save_file(N_path, N)

        try:
            print("开始载入用户协同过滤矩阵....", file=sys.stderr)
            user_sim_matrix = load_file(user_sim_matrix_path)
        except BaseException:
            i = 1
            print(len(item_user))
            for item, users in item_user.items():
                print(i)
                i += 1
                # print('item:' + str(item))
                for u in users:
                    for v in users:
                        if u == v:
                            continue
                        # user_sim_matrix.setdefault(u, defaultdict(int))
                        # user_sim_matrix[u][v] += 1

            save_file(user_sim_matrix_path, user_sim_matrix)
        # print(N)
        # # 计算相关度
        # for u, related_users in user_sim_matrix.items():
        #     for v, con_items_count in related_users.items():
        #         user_sim_matrix[u][v] = con_items_count / math.sqrt(N[u] * N[v])

        return user_sim_matrix

    def recommend(self, user, N, K):
        """推荐
            @param user:   用户
            @param N:    推荐的商品个数
            @param K:    查找最相似的用户个数
            @return: 商品字典 {商品 : 相似性打分情况}
        """
        # related_items = self.train_data.get(user, set)

        recommmens = dict()
        for v, sim in sorted(self.user_sim_matrix.get(user, dict()).items(),
                             key=itemgetter(1), reverse=True)[:K]:
            for item in self.train_data[v]:
                # if item in related_items:
                #     continue
                recommmens.setdefault(item, 0.)
                recommmens[item] += sim

        return dict(sorted(recommmens.items(), key=itemgetter(1), reverse=True)[: N])


    def recommend_users(self, users, N, K):
        """推荐测试集
            @param users:    用户list
            @param N:    推荐的商品个数
            @param K:    查找最相似的用户个数
            @return: 推荐商品字典 {用户 : 推荐的商品的list}, 推荐字典 {用户 : 商品字典 {商品 : 相似性打分情况}}
        """
        item_recommends = dict()
        recommends = dict()
        for user in users:
            user_recommends = self.recommend(user, N, K)
            item_recommends[user] = list(user_recommends.keys())

            recommends[user] = user_recommends

        return item_recommends, recommends



    def init_test(self, origin_data):
        """
        初始化测试数据集
        users:用户集合
        items: {user_id: item}
        ratings {user: {item: rating}}
        """
        users = set()
        items = dict()
        ratings = dict()
        for user, item, rating in origin_data:
            users.add(user)
            items.setdefault(user, set())
            items[user].add(item)

            ratings.setdefault(user, dict())
            ratings[user].update({item: rating})
        return users, items, ratings

    def test_recommend_records(self, tests, recommends):
        """
        推荐评分字典
        records: [[user, item, pre_rating, actual_rating]
        """
        records = []
        for user in recommends.keys():
            test = tests[user].keys()
            for item in recommends[user].keys():
                if item in test:
                    records.append([user, item, tests[user][item], recommends[user][item]])
        return records

    def popular_recommend(self, test_data):
        item_recommends = dict()
        for user, item, rating in test_data:
            item_recommends.setdefault(user, [])
            item_recommends[user].append(item)

        return item_recommends



if __name__ == '__main__':
    base_path = "/Volumes/d/CIKM/round2/"
    train_path = base_path + "buy.csv"
    test_path = base_path + "test.csv"
    user_cf = UserCF(train_path)
    # users, tests, tests_ratings = user_cf.init_test(testset)
    # print(users)
    # # 开始训练
    user_cf.train(sim_matrix_path="store/buy_user_sim.pkl")
    # popular_items = process_data.get_popular_items()
    # item_recommends, recommends = user_cf.recommend_users(users, 30, 1000)
    # item_recommends = user_cf.recommend_users_xgb(users, 30, 10)
    # item_recommends = user_cf.recommend_users_v2(testset, 30, 1000)
    # item_recommends = user_cf.popular_recommend(testset)
    # with open('submission.csv', 'w') as f:
    #     for user, items in item_recommends.items():
    #         if len(items) < 30:
    #             for p_item in popular_items:
    #                 if p_item not in items:
    #                     items.append(p_item)
    #                 if len(items) == 30:
    #                     break
    #         else:
    #             items = items[:30]
    #         ret = str(user) + ','
    #         print(len(items))
    #         for i, item in enumerate(items):
    #
    #             if i == 29:
    #                 ret = ret + str(item)
    #             else:
    #                 ret = ret + str(item) + ','
    #         f.write(ret + '\n')