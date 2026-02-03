# Standard library
from enum import IntEnum
from typing import IO, Optional

from colorama import Fore

class FlowWeaveResult(IntEnum):
    FAIL = 0
    SUCCESS = 1
    IGNORE = 2

class TaskData:
    def __init__(self,
                 name,
                 task_class,
                 option: dict,
                 stage_name: str,
                 flow_part: int,
                 flow_all: int,
                 do_only: str = None,
                 show_log: bool = False):
        self.name = name
        self.task_class = task_class
        self.option = option

        self.stage_name = stage_name
        self.flow_part = flow_part
        self.flow_all = flow_all

        self.do_only = do_only
        self.show_log = show_log

class FlowWeaveTask:
    def __init__(self, prev_future):
        self.prev_future = prev_future
        self.return_data = None

    def __call__(self):
        result, return_data = self.run()
        return result, return_data

    def run(self):
        return FlowWeaveResult.SUCCESS, self.return_data

    def set_task_data(self, task_data: TaskData) -> None:
        self.task_data = task_data

    def message(self,
                *args: object,
                sep: str = " ",
                end: str = "\n",
                file: Optional[IO[str]] = None,
                flush: bool = False) -> None:
        if not self.task_data:
            raise Exception("task_data is 'None'")

        part_num = self.task_data.flow_part
        all_num = self.task_data.flow_all
        stage = self.task_data.stage_name
        task = self.task_data.name
        head = f"[Flow {part_num} / {all_num}] {stage}/{task}"

        args = [f"{head}: {str(a)}" for a in args]

        print(*args, sep=sep, end=end, file=file, flush=flush)

    def error(self,
                *args: object,
                sep: str = " ",
                end: str = "\n",
                file: Optional[IO[str]] = None,
                flush: bool = False) -> None:
        if not self.task_data:
            raise Exception("task_data is 'None'")

        part_num = self.task_data.flow_part
        all_num = self.task_data.flow_all
        stage = self.task_data.stage_name
        task = self.task_data.name
        head = f"[Flow {part_num} / {all_num}] {stage}/{task}"

        args = [f"{head}: {Fore.RED}{str(a)}" for a in args]

        print(*args, sep=sep, end=end, file=file, flush=flush)
