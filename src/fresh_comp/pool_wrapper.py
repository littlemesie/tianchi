# -*- coding: utf-8 -*-

from aiomysql import Pool
from aiomysql import DictCursor


class PoolWrapper(Pool):
    """add cache function"""

    def __init__(self, echo, pool_recycle, loop, mincached=0, maxcached=0,
                 minsize=0, maxsize=0, **kwargs):
        kwargs["cursorclass"] = DictCursor
        super(PoolWrapper, self).__init__(minsize, maxsize, echo, pool_recycle, loop, **kwargs)
        if maxcached < mincached:
            raise ValueError("maxcached should be not less than mincached")
        if maxsize < maxcached:
            raise ValueError("maxsize should be not less than maxcached")
        if minsize < mincached:
            raise ValueError("minsize should be not less than mincached")
        self._mincached = mincached
        self._maxcached = maxcached

    def release(self, conn):
        """Release free connection back to the connection pool.

        This is **NOT** a coroutine.
        """
        fut = self._loop.create_future()
        fut.set_result(None)

        if conn in self._terminated:
            assert conn.closed, conn
            self._terminated.remove(conn)
            return fut
        assert conn in self._used, (conn, self._used)
        self._used.remove(conn)
        if not conn.closed:
            in_trans = conn.get_transaction_status()
            if in_trans:
                conn.close()
                return fut
            if self._closing:
                conn.close()
            elif len(self._free) >= self._maxcached:
                conn.close()
            else:
                self._free.append(conn)
            fut = self._loop.create_task(self._wakeup())
        return fut
