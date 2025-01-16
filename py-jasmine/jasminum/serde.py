import struct
from datetime import date, timedelta
from io import BytesIO

import polars as pl

from .exceptions import JasmineEvalException
from .j import J, JType
from .j_fn import JFn


def serialize(any: J, compress: bool) -> bytes:
    estimated_size = estimate_size(any)
    match any.j_type:
        case JType.NULL:
            return bytes(8)
        case JType.BOOLEAN:
            if any.data:
                return b"\x01\x00\x00\x00\x01\x00\x00\x00"
            else:
                return b"\x01\x00\x00\x00\x00\x00\x00\x00"
        case JType.INT | JType.TIME | JType.DURATION:
            return any.j_type.value.to_bytes(8, "little") + any.data.to_bytes(
                8, "little", signed=True
            )
        case JType.FLOAT:
            return any.j_type.value.to_bytes(8, "little") + struct.pack("<d", any.data)
        case JType.DATE:
            return any.j_type.value.to_bytes(4, "little") + (
                any.data - date(1970, 1, 1)
            ).days.to_bytes(4, "little")
        case JType.DATETIME | JType.TIMESTAMP:
            buf = BytesIO(bytes(estimated_size))
            buf.write(any.j_type.value.to_bytes(4, "little"))
            tz = any.data.tz()
            buf.write((len(tz) + 8).to_bytes(4, "little"))
            buf.write(any.data.as_py().to_bytes(8, "little"))
            buf.write(tz.encode("utf-8"))
            if buf.tell() % 8 != 0:
                buf.write(b"\x00" * (8 - buf.tell() % 8))
            buf.truncate()
            return buf.getvalue()
        case JType.STRING | JType.CAT:
            data = any.data.encode("utf-8")
            buf = BytesIO(bytes(estimated_size))
            buf.write(any.j_type.value.to_bytes(4, "little"))
            buf.write(len(data).to_bytes(4, "little"))
            buf.write(data)
            if buf.tell() % 8 != 0:
                buf.write(b"\x00" * (8 - buf.tell() % 8))
            buf.truncate()
            return buf.getvalue()
        case JType.SERIES | JType.DATAFRAME:
            if any.j_type == JType.SERIES:
                data = any.data.to_frame()
            else:
                data = any.data
            buf = BytesIO(bytes(estimated_size))
            buf.write(any.j_type.value.to_bytes(4, "little"))
            buf.write(bytes(4))
            # 4MB
            if estimated_size > 4000000 and compress:
                data.write_ipc(buf, compression="zstd")
            else:
                data.write_ipc(buf)
            msg_len = buf.tell() - 8
            buf.seek(4)
            buf.write(msg_len.to_bytes(4, "little"))
            buf.seek(msg_len + 8)
            if buf.tell() % 8 != 0:
                buf.write(b"\x00" * (8 - buf.tell() % 8))
            buf.truncate()
            return buf.getvalue()
        case JType.LIST:
            buf = BytesIO(bytes(estimated_size))
            buf.write(any.j_type.value.to_bytes(4, "little"))
            # reserve 4 bytes for length
            buf.write(bytes(4))
            buf.write(len(any).to_bytes(8, "little"))
            for item in any.data:
                buf.write(serialize(item, compress))
            buf.truncate()
            msg_len = buf.tell() - 8
            buf.seek(4)
            buf.write(msg_len.to_bytes(4, "little"))
            buf.seek(msg_len + 8)
            buf.truncate()
            return buf.getvalue()
        case JType.DICT:
            buf = BytesIO(bytes(estimated_size))
            buf.write(any.j_type.value.to_bytes(4, "little"))
            # reserve 4 bytes for length
            buf.write(bytes(4))
            data_len = len(any)
            buf.write(data_len.to_bytes(4, "little"))
            # reserve 4 bytes for keys length
            buf.write(bytes(4))

            key_start_offset = buf.tell()
            buf.seek(key_start_offset + data_len * 4)
            key_value_offset = buf.tell()

            offset_buf = BytesIO(bytes(4 * data_len))
            for k in any.data.keys():
                buf.write(k.encode("utf-8"))
                offset_buf.write((buf.tell() - key_value_offset).to_bytes(4, "little"))

            key_len = buf.tell() - key_start_offset
            if buf.tell() % 8 != 0:
                buf.write(b"\x00" * (8 - buf.tell() % 8))
            value_len_start = buf.tell()
            buf.seek(key_start_offset)
            buf.write(offset_buf.getvalue())

            buf.seek(value_len_start)
            # reserve 4 bytes for values length
            buf.write(bytes(8))
            for v in any.data.values():
                buf.write(serialize(v, compress))
            full_len = buf.tell()
            buf.seek(4)
            buf.write((full_len - 8).to_bytes(4, "little"))
            buf.seek(12)
            buf.write(key_len.to_bytes(4, "little"))
            buf.seek(value_len_start)
            buf.write((full_len - value_len_start - 8).to_bytes(4, "little"))
            buf.seek(full_len)
            buf.truncate()
            return buf.getvalue()
        case JType.FN:
            fn_body = repr(any.data).encode("utf-8")
            buf = BytesIO(bytes(estimated_size))
            buf.write(any.j_type.value.to_bytes(4, "little"))
            buf.write(len(fn_body).to_bytes(4, "little"))
            buf.write(fn_body)
            if buf.tell() % 8 != 0:
                buf.write(b"\x00" * (8 - buf.tell() % 8))
            buf.truncate()
            return buf.getvalue()


def write_len(msg_bytes: bytes, len: int, start: int):
    for i, b in enumerate(len.to_bytes(4, "little")):
        msg_bytes[i + start] = b


def serialize_err(err: str) -> bytes:
    err_bytes = err.encode("utf-8")
    buf = BytesIO(bytes(len(err_bytes) + 13))
    # 1, little-endian
    # 0 - async, 1 - sync, 2 - response
    buf.write(bytes([1, 2, 0, 0]))
    buf.write((len(err_bytes) + 5).to_bytes(4, "little"))
    # 16 - error
    buf.write(b"\x10")
    buf.write(len(err_bytes).to_bytes(4, "little"))
    buf.write(err_bytes)
    return buf.getvalue()


def deserialize(any: bytearray) -> J:
    j_type = JType(any[0])
    data = any[4:]
    match j_type:
        case JType.NULL:
            return J(None)
        case JType.BOOLEAN:
            return J(bool(data[0]), j_type)
        case JType.INT | JType.TIME | JType.DURATION:
            return J(int.from_bytes(data[4:12], "little", signed=True), j_type)
        case JType.FLOAT:
            return J(struct.unpack("<d", data[4:12])[0])
        case JType.DATE:
            return J(
                date(1970, 1, 1) + timedelta(days=int.from_bytes(data[:4], "little")),
                j_type,
            )
        case JType.DATETIME | JType.TIMESTAMP:
            dt_len = int.from_bytes(data[0:4], "little")
            timestamp = int.from_bytes(data[4:12], "little")
            tz = data[12 : dt_len - 4].decode("utf-8")
            return (
                J.from_nanos(timestamp, tz)
                if j_type == JType.TIMESTAMP
                else J.from_millis(timestamp, tz)
            )
        case JType.STRING | JType.CAT:
            str_len = int.from_bytes(data[:4], "little")
            data_str = data[4 : 4 + str_len].decode("utf-8")
            return J(data_str, j_type)
        case JType.SERIES | JType.DATAFRAME:
            msg_len = int.from_bytes(data[:4], "little")
            data = pl.read_ipc(BytesIO(data[4 : 4 + msg_len]))
            return J(data if j_type == JType.DATAFRAME else data.to_series())
        case JType.LIST:
            # byte_len = int.from_bytes(data[:4], "little")
            item_len = int.from_bytes(data[4:8], "little")
            items = []
            # list length(8)
            offset = 12
            for _ in range(item_len):
                sub_byte_len = FIX_LENGTH.get(JType(data[offset]), -1) + 1
                if sub_byte_len == 0:
                    sub_byte_len = (
                        int.from_bytes(data[offset + 4 : offset + 8], "little") + 8
                    )
                item = deserialize(data[offset : offset + sub_byte_len])
                # skip padding
                if sub_byte_len % 8 != 0:
                    sub_byte_len += 8 - sub_byte_len % 8
                items.append(item)
                offset += sub_byte_len
            return J(items)
        case JType.DICT:
            # byte_len = int.from_bytes(data[:4], "little")
            item_len = int.from_bytes(data[4:8], "little")
            # keys_len = int.from_bytes(data[8:12], "little")
            offset = 12
            keys = []
            indices = struct.unpack(
                f"<{item_len}I", data[offset : offset + 4 * item_len]
            )
            offset += 4 * item_len
            current_index = offset
            for index in indices:
                keys.append(data[current_index : offset + index].decode("utf-8"))
                current_index = offset + index
            offset = current_index
            # skip padding
            if offset % 8 != 0:
                offset += 8 - offset % 8
            # values_len = int.from_bytes(data[offset : offset + 4], "little")
            offset += 4
            values = []
            for _ in range(item_len):
                sub_byte_len = FIX_LENGTH.get(JType(data[offset]), -1) + 1
                if sub_byte_len == 0:
                    sub_byte_len = (
                        int.from_bytes(data[offset + 4 : offset + 8], "little") + 8
                    )
                item = deserialize(data[offset : offset + sub_byte_len])
                # skip padding
                if sub_byte_len % 8 != 0:
                    sub_byte_len += 8 - sub_byte_len % 8
                values.append(item)
                offset += sub_byte_len
            return J(dict(zip(keys, values)), j_type)
        case JType.ERR:
            err_len = int.from_bytes(data[:4], "little")
            raise JasmineEvalException(data[4 : 4 + err_len].decode("utf-8"))
        case JType.FN:
            fn_body_length = int.from_bytes(data[:4], "little")
            return J(
                JFn(data[4 : fn_body_length + 4].decode("utf-8"), dict(), [], 0, "")
            )
        case _:
            raise JasmineEvalException(
                f"unsupported j type for deserialization: {j_type}"
            )


FIX_LENGTH = {
    JType.NULL: 7,
    JType.BOOLEAN: 7,
    JType.INT: 15,
    JType.TIME: 15,
    JType.DURATION: 15,
    JType.FLOAT: 15,
    JType.DATE: 7,
}


def estimate_size(any: J) -> int:
    match any.j_type:
        case JType.NULL:
            return 8
        case JType.BOOLEAN:
            return 8
        case JType.INT | JType.TIME | JType.DURATION | JType.FLOAT:
            return 16
        case JType.DATE:
            return 8
        case JType.DATETIME | JType.TIMESTAMP:
            # type code(4), milliseconds|nanoseconds(8), timezone name length(4), time zone name(max 32 chars)
            return 48
        case JType.STRING | JType.CAT:
            return 8 + len(any.data.encode("utf-8")) + 8
        case JType.SERIES | JType.DATAFRAME:
            return 8 + int(any.data.estimated_size() * 1.1) + 8
        case JType.LIST:
            # list type code(4), byte length of j list(4), item length of list(8), each item bytes
            return 16 + sum(estimate_size(item) for item in any.data)
        case JType.DICT:
            # 4 - dict type code
            # 4 - byte length of j dict
            # 4 - item length of dict
            # 4 - byte length of keys
            # 4 * d_len - offsets
            # s - syms
            # ? - padding to align by 8 bytes
            # 8 - length of values - v
            # v - values
            return 32 + sum(
                4 + len(key.encode("utf-8")) + estimate_size(value)
                for key, value in any.data.items()
            )
        case JType.FN:
            return 8 + len(repr(any.data).encode("utf-8"))
        case _:
            raise JasmineEvalException(
                f"unsupported j type for serialization: {any.j_type}"
            )
