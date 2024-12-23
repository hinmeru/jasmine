from .exceptions import JasmineEvalException
from .j import J
from .j_conn import JConn


class JHandle:
    _conn: JConn | object

    def __init__(self, conn: object):
        self._conn = conn

    def sync(self, data: J) -> J:
        if isinstance(self._conn, JConn):
            return self._conn.sync(data)
        else:
            raise JasmineEvalException(
                "not supported 'sync' for connection type %s", type(self._conn)
            )

    def asyn(self, data: J):
        if isinstance(self._conn, JConn):
            return self._conn.asyn(data)
        else:
            raise JasmineEvalException(
                "not supported 'async' for connection type %s", type(self._conn)
            )

    def close(self):
        if isinstance(self._conn, JConn):
            self._conn.disconnect()
        else:
            raise JasmineEvalException(
                "not supported 'close' for connection type %s", type(self._conn)
            )
