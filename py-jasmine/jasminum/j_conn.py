import os
import socket

from . import serde
from .exceptions import JasmineError
from .j import J


class JConn:
    def __init__(self, host, port, user="", password=""):
        if not user:
            try:
                self.user = os.getlogin()
            except Exception:
                self.user = "unknown"
        if (not host) or host == socket.gethostname():
            host = "127.0.0.1"
        self.host = host
        self.port = port
        self.password = password
        if self.password == "":
            self.password = os.getenv("JASMINUM_IPC_TOKEN")
        self.socket = None
        self.is_local = host == "localhost" or host == "127.0.0.1"

    @classmethod
    def from_socket(cls, socket: socket.socket, host: str, port: int) -> "JConn":
        """Create a JConn instance from an existing socket connection."""
        conn = cls(host, port)
        conn.socket = socket
        conn.is_local = host == "localhost" or host == "127.0.0.1"
        return conn

    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        self.socket.send(b"jsm:")
        credential = f"{self.user}:{self.password}".encode("utf-8")
        self.socket.send(len(credential).to_bytes(4, "little"))
        self.socket.sendall(credential)
        ver = self.socket.recv(1)[0]
        if ver == 0:
            raise JasmineError("invalid credential")
        self.socket.setblocking(False)
        # self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 10_485_760)
        # self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 10_485_760)

    def disconnect(self):
        if self.socket:
            self.socket.close()
            self.socket = None

    def sync(self, data: J) -> J:
        if not self.socket:
            self.connect()
        msg_bytes = serde.serialize(data, not self.is_local)
        self.socket.setblocking(True)
        # 0 - async, 1 - sync, 2 - response
        self.socket.send(bytes([1, 1, 0, 0]) + len(msg_bytes).to_bytes(4, "little"))
        self.socket.sendall(msg_bytes)
        response = self.socket.recv(8)
        res_len = int.from_bytes(response[4:], "little")
        response = bytearray(res_len)
        read_bytes = 0
        while read_bytes < res_len:
            memview = memoryview(response)[read_bytes:]
            nread = self.socket.recv_into(memview)
            read_bytes += nread
        self.socket.setblocking(False)
        return serde.deserialize(response)

    def asyn(self, data: J) -> J:
        if not self.socket:
            self.connect()
        msg_bytes = serde.serialize(data, not self.is_local)
        self.socket.setblocking(True)
        # 0 - async, 1 - sync, 2 - response
        self.socket.send(bytes([1, 0, 0, 0]) + len(msg_bytes).to_bytes(4, "little"))
        self.socket.sendall(msg_bytes)
        self.socket.setblocking(False)
        return J(None)
