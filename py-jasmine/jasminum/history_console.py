import atexit
import code
import os
import readline


class HistoryConsole(code.InteractiveConsole):
    def __init__(
        self,
        locals=None,
        filename="<jasminum>",
        histfile=os.path.expanduser("~/.jasminum-history"),
    ):
        code.InteractiveConsole.__init__(self, locals, filename)
        self.init_history(histfile)

    def init_history(self, histfile):
        readline.parse_and_bind("tab: complete")
        if hasattr(readline, "read_history_file"):
            try:
                readline.read_history_file(histfile)
            except FileNotFoundError:
                pass
            atexit.register(self.save_history, histfile)

    def save_history(self, histfile):
        readline.set_history_length(1000)
        readline.write_history_file(histfile)
