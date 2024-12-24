import argparse
import asyncio
import importlib.metadata
import os
import platform
import socket
import traceback

import polars as pl
from termcolor import cprint

from . import serde
from .context import Context
from .engine import Engine
from .eval import eval_ipc, eval_src
from .j_handle import JHandle

pl.Config.set_fmt_str_lengths(80)
pl.Config.set_tbl_rows(20)
pl.Config.set_tbl_cols(20)

__version__ = importlib.metadata.version("jasminum")

parser = argparse.ArgumentParser(description="jasminum, the python engine for jasmine")

parser.add_argument(
    "-d",
    "--debug",
    action="store_true",
    default=False,
    dest="debug",
    help="enable debug mode",
)

parser.add_argument(
    "-f",
    "--file",
    type=str,
    dest="file",
    help="path to the source file to execute",
)

parser.add_argument(
    "-p",
    "--port",
    type=int,
    default=0,
    dest="port",
    help="port number to listen on",
)


async def handle_client(engine: Engine, client: socket.socket, is_local: bool):
    while True:
        try:
            data = await asyncio.get_event_loop().sock_recv(client, 8)
            if not data:
                cprint("client disconnected", "red")
                break
            is_sync = data[1] == 1
            msg_len = int.from_bytes(data[4:], "little")
            data = await asyncio.get_event_loop().sock_recv(client, msg_len)
            # print(f"received {data} bytes from client")
            j = serde.deserialize(data)
            # print(f"received {j} from client")
            try:
                res = eval_ipc(j, engine)
                if is_sync:
                    msg_bytes = serde.serialize(res, not is_local)
                    await asyncio.get_event_loop().sock_sendall(
                        client,
                        bytes([1, 2, 0, 0]) + len(msg_bytes).to_bytes(4, "little"),
                    )
                    await asyncio.get_event_loop().sock_sendall(client, msg_bytes)
            except Exception as e:
                # traceback.print_exc()
                cprint(str(e), "red")
                err_bytes = serde.serialize_err(str(e))
                await asyncio.get_event_loop().sock_sendall(client, err_bytes)
        except Exception as e:
            cprint(e, "red")
            break
    client.close()


async def get_user_input(prompt: str) -> str:
    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(None, input, prompt)
    except EOFError:
        cprint("exit on ctrl+D", "red")
        return "EOFError"


async def handle_user_input(engine: Engine, is_debug=False):
    while True:
        try:
            src = []
            line = await get_user_input("j*  ")
            if line == "EOFError":
                return
            if line == "":
                continue
            else:
                src.append(line)
            while True:
                line = await get_user_input("*   ")
                if line == "EOFError":
                    return
                if not line:
                    break
                src.append(line)
            src = "\n".join(src)
            engine.sources[0] = (src, "")
            try:
                res = eval_src(src, 0, engine, Context(dict()))
                cprint(res, "light_green")
            except Exception as e:
                if is_debug:
                    traceback.print_exc()
                cprint(e, "red")
        except KeyboardInterrupt:
            print()
            continue


async def async_main():
    engine = Engine()
    args = parser.parse_args()

    print(
        """\x1b[1;32m\
           ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣴⣿⣗⠄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
           ⠀⢀⣤⣶⣶⣶⣦⣄⣀⠀⠀⠀⢸⣿⣿⣿⣿⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
           ⠀⣼⣿⣿⣿⣿⣿⣽⣿⣿⣦⡀⣾⣿⣿⣿⣿⣿⢳⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
           ⠀⢸⣾⣻⣿⣿⣿⣿⣿⣿⣿⣷⡜⢿⣿⣿⣿⣿⡝⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
           ⠀⠀⠻⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡼⣿⣿⣿⡿⢡⣴⣾⣿⣶⣿⣷⣦⣄⡀⠀⠀
           ⠀⠀⠀⠀⠙⠻⢿⣿⣿⣿⣿⣿⣿⣷⡽⠿⣛⣽⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣶⡄
           ⠀⠀⠀⠀⠀⠀⠀⢀⣴⣿⣿⣿⣿⣿⣧⣤⣿⡻⠿⠿⢿⣿⣿⠿⠛⠉⠛⠋⠉⠀
           ⠀⠀⠀⠀⠀⠀⢠⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣦⡀⠀⠀⠀⠀⠀⠀⠀
           ⠀⠀⠀⠀⠀⠀⢸⣿⣿⣿⣿⣿⣿⣿⣿⠳⣿⣿⣿⣿⣿⣿⣿⣦⡀⠀⠀⠀⠀⠀
           ⠀⠀⠀⠀⠀⠀⢸⣿⣿⣿⣿⣿⣿⠟⠁⠀⠈⠻⣿⣿⣿⣿⣿⣿⣧⠀⠀⠀⠀⠀
           ⠀⠀⠀⠀⠀⠀⠀⠙⡛⠛⠛⠋⠁⠀⠀⠀⠀⠀⠈⠻⣿⣿⣿⣿⠟⠀⠀⠀⠀⠀
           ⠀⠀⠀⠀⠀⣠⣴⠛⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠛⠉⠀⠀⠀⠀⠀⠀⠀
           ⠀⠀⠀⢠⠟⠉
    ver: {}
    pid: {} \x1b[0m\n""".format(__version__, os.getpid())
    )

    # readline doesn't work for windows
    if platform.system() != "Windows":
        import readline

        from .history_console import HistoryConsole

        HistoryConsole()

        readline.set_completer(engine.complete)

    task = asyncio.create_task(handle_user_input(engine, args.debug))

    if args.port > 0:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", args.port))
        server.listen()
        server.setblocking(False)
        loop = asyncio.get_event_loop()
        cprint(
            "    listen and serve on 0.0.0.0:%s\n" % args.port, "green", attrs=["bold"]
        )
        try:
            while True:
                try:
                    client, addr = await loop.sock_accept(server)
                    cprint(f"accepted connection from {addr}", "green")
                    engine.set_handle(
                        engine.get_min_handle_id(),
                        JHandle(None, "incoming", addr[0], addr[1]),
                    )
                    asyncio.create_task(
                        handle_client(engine, client, str(addr) == "127.0.0.1")
                    )
                except asyncio.exceptions.CancelledError:
                    break
        except KeyboardInterrupt:
            tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            server.close()

    await task


def main():
    asyncio.run(async_main())
