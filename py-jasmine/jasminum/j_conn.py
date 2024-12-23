import os
import socket

from . import serde
from .j import J


class JConn:
    def __init__(self, host, port, user="", password=""):
        if not user:
            try:
                user = os.getlogin()
            except Exception:
                user = "unknown"
        if (not host) or host == socket.gethostname():
            host = "127.0.0.1"
        self.host = host
        self.port = port
        self.password = password
        self.socket = None
        self.is_local = host == "localhost" or host == "127.0.0.1"

    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))

    def disconnect(self):
        if self.socket:
            self.socket.close()
            self.socket = None

    def sync(self, data: J) -> J:
        if not self.socket:
            self.connect()
        msg_bytes = serde.serialize(data, not self.is_local)
        # 0 - async, 1 - sync, 2 - response
        self.socket.send(bytes([1, 1, 0, 0]) + len(msg_bytes).to_bytes(4, "little"))
        self.socket.sendall(msg_bytes)
        response = self.socket.recv(8)
        res_len = int.from_bytes(response[4:], "little")
        response = self.socket.recv(res_len)
        return serde.deserialize(response)

    def asyn(self, data: J):
        if not self.socket:
            self.connect()
        msg_bytes = serde.serialize(data, not self.is_local)
        # 0 - async, 1 - sync, 2 - response
        self.socket.send(bytes([1, 0, 0, 0]) + len(msg_bytes).to_bytes(4, "little"))
        self.socket.sendall(msg_bytes)
        response = self.socket.recv(8)
        res_len = int.from_bytes(response[4:], "little")
        response = self.socket.recv(res_len)
        return serde.deserialize(response)
