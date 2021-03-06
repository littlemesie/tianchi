# ! /usr/bin/python3
# coding=utf-8
import pandas as pd
from collections import defaultdict
import math
from operator import itemgetter
import sys
sys.path.extend(['/Users/t/python/tianchi', '/Users/t/python/tianchi/src'])
from utils.utils import load_file, save_file
from antai import process_data



class UserCF(object):
    """用户协同过滤"""

    def __init__(self):
        self.train_data = dict()

    def train(self, origin_data, sim_matrix_path="store/user_sim.pkl"):
        """训练模型
            @param origin_data: 原始数据
            @param sim_matrix_path:  协同矩阵保存的路径
        """
        self.origin_data = origin_data
        # 初始化训练集
        self._init_train(origin_data)
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

    def _init_train(self, origin_data):
        """初始化训练集数据"""
        for user, item, _ in origin_data:
            self.train_data.setdefault(user, set())
            self.train_data[user].add(item)

    def user_similarity(self):
        """建立用户的协同过滤矩阵"""
        # 建立用户倒排表
        item_user = dict()
        for user, items in self.train_data.items():
            print('user:' + str(user))
            for item in items:
                item_user.setdefault(item, set())
                item_user[item].add(user)

        # 建立用户协同过滤矩阵
        user_sim_matrix = dict()
        N = defaultdict(int)  # 记录用户购买商品数
        for item, users in item_user.items():
            print('item:' + str(item))
            for u in users:
                N[u] += 1
                for v in users:
                    if u == v:
                        continue
                    user_sim_matrix.setdefault(u, defaultdict(int))
                    user_sim_matrix[u][v] += 1

        # 计算相关度
        for u, related_users in user_sim_matrix.items():
            for v, con_items_count in related_users.items():
                user_sim_matrix[u][v] = con_items_count / math.sqrt(N[u] * N[v])

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

    def recommend_v2(self, user, N, K, items):
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
                if item in items:
                    continue
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

    def recommend_users_v2(self, test_data, N, K):
        """推荐测试集
            @param test_data:   测试数据
            @param N:    推荐的商品个数
            @param K:    查找最相似的用户个数
            @return: 推荐商品字典 {用户 : 推荐的商品的list}, 推荐字典 {用户 : 商品字典 {商品 : 相似性打分情况}}
        """
        item_recommends = dict()
        for user, item, rating in test_data:
            item_recommends.setdefault(user, [])
            item_recommends[user].append(item)
            if len(item_recommends[user]) < N:
                N1 = N - len(item_recommends[user])
                recommends = self.recommend_v2(user, N1, K, item_recommends[user])

                item_recommends[user] = list(item_recommends[user]) + list(recommends.keys())


        return item_recommends

    def recommend_users_xgb(self, users, N, K):
        """推荐测试集
            @param users:    用户list
            @param N:    推荐的商品个数
            @param K:    查找最相似的用户个数
            @return: 推荐商品字典 {用户 : 推荐的商品的list}, 推荐字典 {用户 : 商品字典 {商品 : 相似性打分情况}}
        """
        xgb_score = pd.read_csv(base_path + 'result.csv')
        recommends = dict()
        print('user:' + str(len(users)))
        for user in users:
            recommends_score = dict()
            for v, sim in sorted(self.user_sim_matrix.get(user, dict()).items(), key=itemgetter(1), reverse=True)[:K]:
                for item in self.train_data[v]:
                    score = xgb_score.loc[xgb_score.item_id == item]
                    if score.empty:
                        continue
                    recommends_score.setdefault(item, 0.)
                    recommends_score[item] = score['prob'].values[0]
            # print(recommends_score)
            print(user)
            topN = dict(sorted(recommends_score.items(), key=itemgetter(1), reverse=True)[: N])
            recommends[user] = list(topN.keys())

        return recommends


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
    base_path = "/Volumes/d/antai/"
    train_path = base_path + "Antai_AE_round1_train_20190626.csv"
    test_path = base_path + "Antai_AE_round1_test_20190626.csv"
    trainset, _ = process_data.read_rating_data(train_path, train_rate=1)
    testset, _ = process_data.read_rating_data(test_path, train_rate=1)
    data = trainset + testset
    user_cf = UserCF()
    # users, tests, tests_ratings = user_cf.init_test(testset)
    # print(users)
    # # 开始训练
    user_cf.train(data)
    popular_items = process_data.get_popular_items()
    # item_recommends, recommends = user_cf.recommend_users(users, 30, 1000)
    # item_recommends = user_cf.recommend_users_xgb(users, 30, 10)
    # item_recommends = user_cf.recommend_users_v2(testset, 30, 1000)
    item_recommends = user_cf.popular_recommend(testset)
    with open('submission.csv', 'w') as f:
        for user, items in item_recommends.items():
            if len(items) < 30:
                for p_item in popular_items:
                    if p_item not in items:
                        items.append(p_item)
                    if len(items) == 30:
                        break
            else:
                items = items[:30]
            ret = str(user) + ','
            print(len(items))
            for i, item in enumerate(items):

                if i == 29:
                    ret = ret + str(item)
                else:
                    ret = ret + str(item) + ','
            f.write(ret + '\n')