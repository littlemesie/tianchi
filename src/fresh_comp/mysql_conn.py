# -*- coding: utf-8 -*-

import asyncio


class MysqlConn(object):
    pool = None

    def __init__(self, loop=asyncio.get_event_loop()):
        self._conn = None
        self._cur = None

    async def select_one(self, sql, params=None):
        try:
            count = await self._cur.execute(sql, params)
            if count > 0:
                return await self._cur.fetchone()
            else:
                return None
        except Exception as e:
            await self.rollback()
            raise e

    async def select_one_value(self, sql, params=None):
        try:
            count = await self._cur.execute(sql, params)
            if count > 0:
                result = await self._cur.fetchone()
                return list(result.values())[0]
            else:
                return None
        except Exception as e:
            await self.rollback()
            raise e

    async def select_many(self, sql, params=None):
        try:
            count = await self._cur.execute(sql, params)
            if count > 0:
                return await self._cur.fetchall()
            else:
                return []
        except Exception as e:
            await self.rollback()
            raise e

    async def select_many_one_value(self, sql, params=None):
        try:
            count = await self._cur.execute(sql, params)
            if count > 0:
                result = await self._cur.fetchall()
                return list(map(lambda one: list(one.values())[0], result))
            else:
                return []
        except Exception as e:
            await self.rollback()
            raise e

    async def insert_one(self, sql, params=None, return_auto_increament_id=False):
        try:
            await self._cur.execute(sql, params)
            if return_auto_increament_id:
                return self._cur.lastrowid
        except Exception as e:
            await self.rollback()
            raise e

    async def insert_many(self, sql, params):
        try:
            count = await self._cur.executemany(sql, params)
            return count
        except Exception as e:
            await self.rollback()
            raise e

    async def update(self, sql, params=None):
        try:
            result = await self._cur.execute(sql, params)
            return result
        except Exception as e:
            await self.rollback()
            raise e

    async def delete(self, sql, params=None):
        try:
            result = await self._cur.execute(sql, params)
            return result
        except Exception as e:
            await self.rollback()
            raise e

    async def begin(self):
        await self._conn.begin()

    async def commit(self):
        try:
            await self._conn.commit()
        except Exception as e:
            await self.rollback()
            raise e

    async def rollback(self):
        await self._conn.rollback()

    async def close(self):
        self.pool.close()
        await self.pool.wait_closed()

    def executed(self):
        return self._cur._executed

    async def __aenter__(self):
        self._conn = await self.pool.acquire()
        self._cur = await self._conn.cursor()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.commit()
        await self._cur.close()
        await self.pool.release(self._conn)
