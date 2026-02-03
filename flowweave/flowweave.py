# Standard library
from functools import reduce
import importlib
from importlib.resources import files
import inspect
import itertools
import json
import logging
from pathlib import Path
import pickle
import sys

# Third-party
import jsonschema
import yaml
from prefect import flow, task, get_run_logger

# Local application / relative imports
from .base import FlowWeaveResult, TaskData, FlowWeaveTask
from .message import FlowMessage

class StageData():
    def __init__(self, name: str, stage_info: dict, default_option: dict, global_option: dict, op_dic: dict, flow_part: int, flow_all: int):
        self.name = name
        self.stage_info = stage_info
        self.default_option = default_option
        self.global_option = global_option
        self.op_dic = op_dic

        self.flow_part = flow_part
        self.flow_all = flow_all

    def __str__(self):
        text = "== Stage ==\n"
        text += f"Name: {self.name}\n"
        text += f"Stage Info: {self.stage_info}\n"
        text += f"Default: {self.default_option}\n"
        text += f"Global: {self.global_option}\n"
        text += f"Operation: {self.op_dic}\n"
        text += "==========="
        return text

class TaskRunner():
    @task
    def start(prev_future, task_data: TaskData):
        if hasattr(prev_future, "result"):
            prev_future = prev_future.result()

        return_data = None
        try:
            task_instance = task_data.task_class(prev_future)
        except AttributeError:
            raise TypeError(f"Failed to get instance of '{task_data.task_class}'")

        # set task member variables
        setattr(task_instance, "task_data", task_data)
        # options
        for key, value in task_data.option.items():
            if hasattr(task_instance, key):
                setattr(task_instance, key, value)
                if task_data.show_log:
                    FlowWeave._print_log(f"Task option {key} found: store {value}")
            else:
                if task_data.show_log:
                    FlowWeave._print_log(f"Task option {key} not found: ignore")
        run_task = True

        if prev_future is not None:
            if "pre_success" == task_data.do_only:
                run_task = True if (FlowWeaveResult.SUCCESS == prev_future.get("result")) else False
            elif "pre_fail" == task_data.do_only:
                run_task = True if (FlowWeaveResult.FAIL == prev_future.get("result")) else False

        if run_task:
            TaskRunner.message_task_start(prev_future, task_data)

            try:
                task_result, return_data = task_instance()
            except Exception as e:
                FlowMessage.error(e)
                task_result = FlowWeaveResult.FAIL

            FlowMessage.task_end(task_data, task_result)
        else:
            TaskRunner.message_task_ignore(prev_future, task_data)
            task_result = FlowWeaveResult.IGNORE

        try:
            if return_data is not None:
                pickle.dumps(return_data)
        except Exception:
            raise TypeError(f"Task '{task_data.name}' return_data is not serializable")

        return {"name" : task_data.name, "option" : task_data.option, "data" : return_data, "result" : task_result}

    def message_task_start(prev_future, task_data: TaskData):
        if prev_future is not None:
            prev_task_name = prev_future.get("name")
            FlowMessage.task_start_link(prev_task_name, task_data)
        else:
            FlowMessage.task_start(task_data)

    def message_task_ignore(prev_future, task_data: TaskData):
        if prev_future is not None:
            prev_task_name = prev_future.get("name")
            FlowMessage.task_ignore_link(task_data, prev_task_name)
        else:
            FlowMessage.task_ignore(task_data)

class FlowWeave():
    @flow
    def run(setting_file: str, parallel: bool = False, show_log: bool = False) -> list[FlowWeaveResult]:
        if not show_log:
            logging.getLogger("prefect").setLevel(logging.CRITICAL)

        flow_data = FlowWeave.load_and_validate_schema(setting_file, "flow")

        op_dic = FlowWeave.get_op_dic(flow_data)

        global_option = flow_data.get("global_option")
        comb_list = list()
        if global_option:
            comb_list = FlowWeave._get_global_option_comb(global_option)
        else:
            comb_list = [{}]

        comb_count = 0
        all_count = len(comb_list)
        futures = []
        results = []
        for comb in comb_list:
            comb_count += 1
            FlowMessage.flow_start(comb_count, all_count)
            FlowMessage.flow_message(comb_count, all_count, comb)
            if parallel:
                futures.append(FlowWeave.run_flow.submit(flow_data=flow_data,
                                                         global_cmb=comb,
                                                         op_dic=op_dic,
                                                         part=comb_count,
                                                         all=all_count,
                                                         show_log=show_log))
            else:
                result = FlowWeave.run_flow(flow_data=flow_data,
                                            global_cmb=comb,
                                            op_dic=op_dic,
                                            part=comb_count,
                                            all=all_count,
                                            show_log=show_log)
                results.append(result)
                FlowMessage.flow_end(comb_count, all_count, result)

        if parallel:
            comb_count = 0
            for f in futures:
                comb_count += 1
                result = f.result()
                results.append(result)
                FlowMessage.flow_end(comb_count, all_count, result)

        return results

    def load_and_validate_schema(file: str, schema: str) -> dict:
        data = FlowWeave._load_yaml(file)
        schema = FlowWeave._load_schema(schema)
        jsonschema.validate(instance=data, schema=schema)

        return data

    def _load_yaml(path: str) -> dict:
        file_path = Path(path)
        if not file_path.exists() or not file_path.is_file():
            raise FileNotFoundError(f"{file_path.resolve()} does not exist or not file")

        with open(Path(path), "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data

    def _load_schema(schema: str) -> dict:
        schema_path = files("flowweave")/ "schema" / f"{schema}.json"
        with schema_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        return data

    def get_op_dic(flow_data: dict, info: bool = False):
        return_dic = dict()

        op_source = flow_data.get("op_source")
        op_source_list = op_source if isinstance(op_source, list) else [op_source]
        for source in op_source_list:
            source_name = f"task/{source}"
            setting_file = f"{source_name.replace('.', '/')}/op_code.yml"
            return_dic |= FlowWeave._get_op_dic_from_setting_file(setting_file, info=info)

        return return_dic

    def get_available_op_dic():
        return_dic = dict()

        base_path = Path("task")
        avaliable_settings = [str(f) for f in base_path.rglob("op_code.yml")]
        for setting in avaliable_settings:
            place = setting.replace("\\", ".").removeprefix("task.").removesuffix(".op_code.yml")
            return_dic[place] = FlowWeave._get_op_dic_from_setting_file(setting.replace("\\", "/"), info=True)

        return return_dic

    def _get_op_dic_from_setting_file(setting_file: str, info: bool = False):
        return_dic = dict()

        setting = FlowWeave.load_and_validate_schema(setting_file, "op_code")
        source_name = setting_file.removesuffix("/op_code.yml").replace("/", ".")

        task_root = Path("task").resolve()
        if str(task_root.parent) not in sys.path:
            sys.path.insert(0, str(task_root.parent))

        op_dic = setting.get("op", {})
        for op, op_info in op_dic.items():
            script_name = op_info.get('script')
            op_class = FlowWeave._get_op_class(source_name, script_name, FlowWeaveTask)

            return_dic[str(op)] = op_class

        return return_dic

    def _get_op_class(source_name: str, script_name: str, base_class):
        module_name = f"{source_name}.{script_name}"

        try:
            module = importlib.import_module(module_name)
        except Exception as e:
            raise RuntimeError(f"Failed to import {module_name}: {e}")

        candidates = []

        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ != module.__name__:
                continue

            if issubclass(obj, base_class) and obj is not base_class:
                candidates.append(obj)

        if len(candidates) == 0:
            raise RuntimeError(
                f"No subclass of {base_class.__name__} found in {module_name}"
            )

        if len(candidates) > 1:
            names = ", ".join(c.__name__ for c in candidates)
            raise RuntimeError(
                f"Multiple subclasses of {base_class.__name__} found in {module_name}: {names}"
            )

        cls = candidates[0]

        return cls

    def _get_global_option_comb(global_option: dict) -> list:
        keys = list(global_option.keys())
        value_lists = []

        for key in keys:
            inner_dict = global_option[key]
            value_lists.append([dict(zip(inner_dict.keys(), v))
                                for v in itertools.product(*inner_dict.values())])

        all_combinations = []
        for combo in itertools.product(*value_lists):
            combined = dict(zip(keys, combo))
            all_combinations.append(combined)

        return all_combinations

    @task
    def run_flow(flow_data: dict, global_cmb: dict, op_dic: dict, part: int, all: int, show_log: bool = False) -> FlowWeaveResult:
        flow_result = FlowWeaveResult.SUCCESS

        if show_log:
            text = "= Flow =\n"
            text += f"Stage: {flow_data.get('flow')}\n"
            text += "========"
            FlowWeave._print_log(text)

        default_option = flow_data.get("default_option", {})

        stage_list = flow_data.get("flow")
        for stage in stage_list:
            FlowMessage.stage_start(stage, part, all)

            stage_info = flow_data.get("stage", {}).get(stage)
            stage_global_option = FlowWeave._get_stage_global_option(global_cmb, stage)

            stage_data = StageData(stage, stage_info, default_option, stage_global_option, op_dic, part, all)
            if show_log:
                FlowWeave._print_log(str(stage_data))

            result = FlowWeave._run_stage(stage_data, show_log)
            if FlowWeaveResult.FAIL == result:
                flow_result = FlowWeaveResult.FAIL

            FlowMessage.stage_end(stage, part, all, result)

        return flow_result

    def _get_stage_global_option(global_cmb: dict, stage: str) -> dict:
        stage_global_option = dict()

        for stages, option in global_cmb.items():
            stage_list = [x.strip() for x in stages.split(",")]
            if stage in stage_list:
                stage_global_option |= option

        return stage_global_option

    def _print_log(text: str):
        logger = get_run_logger()
        stage_text_list = text.split("\n")
        for text in stage_text_list:
            logger.info(f"{text}")

    def _run_stage(stage_data: StageData, show_log: bool = False):
        stage_result = FlowWeaveResult.SUCCESS

        all_futures = []

        for task_name, task_dic in stage_data.stage_info.items():
            part = task_dic.get("chain", {}).get("part", "head")
            if "head" == part:
                all_futures.extend(
                    FlowWeave._run_task(stage_data, task_name, None, None, show_log)
                )

        for f in all_futures:
            result = f.result()
            if FlowWeaveResult.FAIL == result.get("result"):
                stage_result = FlowWeaveResult.FAIL

        return stage_result

    def _deep_merge(a: dict, b: dict) -> dict:
        result = copy.deepcopy(a)
        for k, v in b.items():
            if k in result and isinstance(result[k], dict) and isinstance(v, dict):
                result[k] = FlowWeave._deep_merge(result[k], v)
            else:
                result[k] = v
        return result

    def _deep_merge_many(*dicts):
        return reduce(FlowWeave._deep_merge, dicts)

    def _run_task(stage_data: dict, task_name: str, prev_future = None, visited = None, show_log: bool = False):
        if visited is None:
            visited = set()
        if task_name in visited:
            raise Exception(f"Cycle detected at task '{task_name}' in {visited}")
        visited.add(task_name)

        try:
            task_dic = stage_data.stage_info.get(task_name)
            if task_dic is None:
                raise KeyError(f"Task '{task_name}' not found in stage '{stage_data.name}'")

            task_module = stage_data.op_dic.get(task_dic.get('op'))
            if not task_module:
                raise Exception(f"module of op '{task_dic.get('op')}' for '{task_name}' not found")

            default_option = stage_data.default_option or {}
            global_option = stage_data.global_option or {}
            task_option = FlowWeave._deep_merge_many(default_option, global_option, task_dic.get("option", {}))

            task_data = TaskData(name=task_name,
                                task_class=task_module,
                                option=task_option,
                                stage_name=stage_data.name,
                                flow_part=stage_data.flow_part,
                                flow_all=stage_data.flow_all,
                                do_only=task_dic.get("do_only"),
                                show_log=show_log)
            if prev_future is None:
                future = TaskRunner.start.submit(None, task_data)
            else:
                future = TaskRunner.start.submit(prev_future, task_data)

            links = task_dic.get("chain", {}).get("next", [])
            links = links if isinstance(links, list) else [links]

            futures = [future]
            for link in links:
                futures.extend(
                    FlowWeave._run_task(stage_data, link, future, visited.copy(), show_log)
                )

            return futures

        finally:
            visited.remove(task_name)