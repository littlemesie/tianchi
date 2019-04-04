import asyncio
from fresh_comp.mysql_conn import MysqlConn
from fresh_comp.pool_wrapper import PoolWrapper
loop = asyncio.get_event_loop()
"""使用例子"""
class Base_MysqlConn(MysqlConn):
    pool = PoolWrapper(mincached=1,
                       maxcached=5,
                       minsize=1,
                       maxsize=10,
                       loop=loop,
                       echo=False,
                       pool_recycle=3600,
                       host='52.9.193.112',
                       user='root',
                       password='123456',
                       db='midatadb',
                       port=3306,
                       )

    """docstring for Base_MysqlConn"""

    def __init__(self):
        super(Base_MysqlConn, self).__init__()

async def insert(data):
    async with Base_MysqlConn() as conn:
        await conn.insert_one(
            "INSERT INTO midatadb.r_security_train(`user_id`, `item_id`, `behavior_type`, `user_geohash`,`item_category`,`time`)"
            " VALUES(%s,%s,%s,%s,%s,%s)", data
        )
        await conn.commit()

base_path ="/Volumes/d/fresh_comp_offline/"
async def read_train_user_data(path=base_path + "tianchi_fresh_comp_train_user.csv"):
    from fresh_comp import utils
    for line in utils.loadfile(filename=path):
        line = line.split(' ')
        arr_list = line[0].split(',')
        print(line)
        await insert(arr_list)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(read_train_user_data())