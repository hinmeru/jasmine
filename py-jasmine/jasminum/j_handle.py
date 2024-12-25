from typing import Literal

from .exceptions import JasmineEvalException
from .j import J, JType
from .j_conn import JConn


class JHandle:
    _conn: JConn | object
    _type: str
    _host: str
    _port: int
    _direction: Literal["int", "out"]

    def __init__(
        self,
        conn: object,
        conn_type="jasmine",
        host="127.0.0.1",
        port=0,
        direction=Literal["in", "out"],
    ):
        self._conn = conn
        self._type = conn_type
        self._host = host
        self._port = port
        self._direction = direction

    def sync(self, query: J) -> J:
        if isinstance(self._conn, JConn):
            return self._conn.sync(query)
        elif self._type == "duckdb":
            if query.j_type == JType.STRING:
                return J(self._conn.sql(query.to_str()).pl())
            else:
                raise JasmineEvalException(
                    "only support 'string' for 'duckdb' connection"
                )
        else:
            raise JasmineEvalException(
                "not supported 'sync' for connection type %s" % type(self._conn)
            )

    def asyn(self, query: J):
        if isinstance(self._conn, JConn):
            return self._conn.asyn(query)
        else:
            raise JasmineEvalException(
                "not supported 'async' for connection type %s" % type(self._conn)
            )

    def close(self):
        if isinstance(self._conn, JConn):
            self._conn.disconnect()
        elif self._type == "duckdb":
            self._conn.close()
        else:
            raise JasmineEvalException(
                "not supported 'close' for connection type %s" % type(self._conn)
            )
