import argparse
import asyncio
import importlib.metadata
import os
import platform
import socket
import traceback
from datetime import datetime, timedelta

import polars as pl
from termcolor import cprint

from .context import Context
from .engine import Engine
from .eval import eval_fn, eval_src, handle_ipc
from .j_conn import JConn
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

parser.add_argument(
    "-t",
    "--timer",
    type=float,
    default=0,
    dest="timer",
    help="timer interval in seconds",
)


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


async def timer_task(engine: Engine, sleep_time_seconds: float, is_debug=False):
    while True:
        await asyncio.sleep(sleep_time_seconds)
        # print("execute timer jobs")
        for task in engine.timer_tasks.values():
            try:
                now = datetime.now().astimezone()
                if (
                    task.is_active
                    and task.next_run < now
                    and (task.end_time is None or task.end_time > now)
                ):
                    eval_fn(
                        task.function,
                        engine,
                        Context(dict()),
                        -1,
                        0,
                        *task.args,
                    )
                    next_run = now + timedelta(microseconds=task.interval // 1000)

                    if task.end_time is not None and next_run > task.end_time:
                        task.is_active = False
                        task.next_run = task.end_time
                    else:
                        task.next_run = next_run
                    task.last_run = now
            except Exception as e:
                if is_debug:
                    traceback.print_exc()
                cprint(f"error executing timer job: {e}", "red")


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

    pl.enable_string_cache()
    # readline doesn't work for windows
    if platform.system() != "Windows":
        import readline

        from .history_console import HistoryConsole

        HistoryConsole()

        readline.set_completer(engine.complete)

    if args.timer > 0:
        task = asyncio.create_task(timer_task(engine, args.timer, args.debug))
        engine.set_timer_task(task)

    task = asyncio.create_task(handle_user_input(engine, args.debug))

    if args.port > 0:
        cprint(
            "    listen and serve on 0.0.0.0:%s\n" % args.port, "green", attrs=["bold"]
        )
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server.bind(("0.0.0.0", args.port))
            server.listen()
        except Exception as e:
            cprint(e, "red")
            exit(1)
        server.setblocking(False)
        loop = asyncio.get_event_loop()
        try:
            while True:
                try:
                    client, addr = await loop.sock_accept(server)
                    cprint(f"accepted connection from {addr}", "green")
                    handle_id = engine.get_max_handle_id()
                    engine.set_handle(
                        handle_id,
                        JHandle(
                            JConn.from_socket(client, addr[0], addr[1]),
                            "jasmine",
                            addr[0],
                            addr[1],
                            "in",
                        ),
                    )
                    asyncio.create_task(
                        handle_ipc(
                            engine,
                            client,
                            str(addr) == "127.0.0.1",
                            handle_id,
                            args.debug,
                        )
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
