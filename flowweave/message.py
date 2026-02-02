# Standard library
from typing import IO, Optional

# Third-party
from colorama import Fore

# Local application / relative imports
from .base import Result, TaskData

class FlowMessage:
    @staticmethod
    def _print(*args: object,
               sep: str = " ",
               end: str = "\n",
               file: Optional[IO[str]] = None,
               flush: bool = False) -> None:
        print(*args, sep=sep, end=end, file=file, flush=flush)

    @staticmethod
    def get_result_text(result: Result) -> str:
        text = ""

        if Result.SUCCESS == result:
            text = f"{Fore.GREEN}SUCCESS"
        elif Result.IGNORE == result:
            text = f"{Fore.CYAN}IGNORE"
        elif Result.FAIL == result:
            text = f"{Fore.RED}FAIL"
        else:
            text = f"{Fore.MAGENTA}UNKNOWN: {result.name}({result.value})"

        return text

    @staticmethod
    def flow_start(part: int, all: int) -> None:
        text = f"{Fore.YELLOW}[Flow {part} / {all}] Start"
        FlowMessage._print(text)

    @staticmethod
    def flow_message(part: int, all: int, global_option: dict) -> None:
        text = f"[Flow {part} / {all}] {global_option}"
        FlowMessage._print(text)

    @staticmethod
    def flow_end(part: int, all: int, result: Result) -> None:
        result_text = FlowMessage.get_result_text(result)
        text = f"{Fore.YELLOW}[Flow {part} / {all}] Finish - {result_text}"
        FlowMessage._print(text)

    @staticmethod
    def stage_start(stage: str, part: int, all: int) -> None:
        text = f"{Fore.MAGENTA}[Flow {part} / {all}] Start Stage {stage}"
        FlowMessage._print(text)

    @staticmethod
    def stage_end(stage: str, part: int, all: int, result: Result) -> None:
        result_text = FlowMessage.get_result_text(result)
        text = f"{Fore.MAGENTA}[Flow {part} / {all}] Finish Stage {stage} - {result_text}"
        FlowMessage._print(text)

    @staticmethod
    def task_start(task_data: TaskData) -> None:
        text = f"{Fore.CYAN}[Flow {task_data.flow_part} / {task_data.flow_all}] Start Task {task_data.stage_name}/{task_data.name}"
        FlowMessage._print(text)

    @staticmethod
    def task_start_link(prev_task: str, task_data: TaskData) -> None:
        text = f"{Fore.CYAN}[Flow {task_data.flow_part} / {task_data.flow_all}] Start Link Task {task_data.stage_name}/{prev_task} -> {task_data.name}"
        FlowMessage._print(text)

    @staticmethod
    def task_ignore(task_data: TaskData) -> None:
        text = f"{Fore.CYAN}[Flow {task_data.flow_part} / {task_data.flow_all}] Ignore {task_data.name} (do_only : {task_data.do_only})"
        FlowMessage._print(text)

    @staticmethod
    def task_ignore_link(task_data: TaskData, prev_task_name: str) -> None:
        text = f"{Fore.CYAN}[Flow {task_data.flow_part} / {task_data.flow_all}] Ignore {prev_task_name} -> {task_data.name} (do_only : {task_data.do_only})"
        FlowMessage._print(text)

    @staticmethod
    def task_end(task_data: TaskData, result: Result) -> None:
        result_text = FlowMessage.get_result_text(result)
        text = f"{Fore.CYAN}[Flow {task_data.flow_part} / {task_data.flow_all}] Finish Task {task_data.stage_name}/{task_data.name} - {result_text}"
        FlowMessage._print(text)

    @staticmethod
    def error(*args: object,
              sep: str = " ",
              end: str = "\n",
              file: Optional[IO[str]] = None,
              flush: bool = False) -> None:
        args = [f"{Fore.RED}{str(a)}" for a in args]
        FlowMessage._print(*args, sep=sep, end=end, file=file, flush=flush)
