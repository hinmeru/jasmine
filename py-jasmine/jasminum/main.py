import argparse
import importlib.metadata
import os
import platform
import traceback

import polars as pl
from termcolor import cprint

from .context import Context
from .engine import Engine
from .eval import eval_src

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


def main():
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

    engine = Engine()
    src = ""
    # readline doesn't work for windows
    if platform.system() != "Windows":
        import readline

        from .history_console import HistoryConsole

        HistoryConsole()

        readline.set_completer(engine.complete)
    while src != "exit":
        try:
            src = []
            line = input("j*  ")
            if line == "":
                continue
            else:
                src.append(line)
            while True:
                line = input("*   ")
                if not line:
                    break
                src.append(line)
            src = "\n".join(src)
            engine.sources[0] = (src, "")
            try:
                res = eval_src(src, 0, engine, Context(dict()))
                cprint(res, "light_green")
            except Exception as e:
                if args.debug:
                    traceback.print_exc()
                cprint(e, "red")
        except EOFError:
            cprint("exit on ctrl+D", "red")
            exit(0)
        except KeyboardInterrupt:
            print()
            continue
