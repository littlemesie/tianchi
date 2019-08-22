# coding=utf-8
import pandas as pd
from antai.round2 import process_data



# 获取top30的item
def get_item_list(df):
    dic = {}
    for item in df[['buyer_admin_id', 'item_id']].values:

        dic.setdefault(int(item[0]), set())
        dic[int(item[0])].add(int(item[1]))

    return dic



if __name__ == '__main__':
    popular_items = process_data.get_popular_items()
    test = pd.read_csv('/Volumes/d/antai/round2/Antai_AE_round2_test_20190813.csv')
    # test = test.sort_values(['buyer_admin_id', 'irank'])
    item_recommends = get_item_list(test)

    with open('submission.csv', 'w') as f:
        for user, items in item_recommends.items():
            items = list(items)
            print(user)
            if len(items) < 30:
                for p_item in popular_items:
                    if p_item not in items:
                        items.append(p_item)
                    if len(items) == 30:
                        break
            else:
                items = items[:30]
            ret = str(user) + ','
            # print(len(items))
            for i, item in enumerate(items):

                if i == 29:
                    ret = ret + str(item)
                else:
                    ret = ret + str(item) + ','
            f.write(ret + '\n')