# Standard library
import argparse
import json
from pathlib import Path

# Third-party
import colorama

# Local application / relative imports
from .flowweave import FlowWeave
from .base import Result

def get_setting_path(args):
    setting_path = None

    if args.flow_file:
        setting_path = str(Path("flow") / f"{args.flow_file}.yml")

    return setting_path

def serialize(obj) -> str:
    if isinstance(obj, type):
        return obj.__name__
    raise TypeError(f"Type {type(obj)} not serializable")

def show_flow_op(setting_path: str, flow_name: str) -> None:
    flow_data = FlowWeave.load_and_validate_schema(file=setting_path, schema="flow")
    op_dic = FlowWeave.get_op_dic(flow_data)
    print_op_dic(op_dic, flow_name)

def show_available_op() -> None:
    op_dic = FlowWeave.get_available_op_dic()
    print_op_dic(op_dic)

def print_op_dic(op_dic: dict, flow_name: str = "") -> None:
    flow_text = ""
    if "" != flow_name:
        flow_text = f"in {flow_name} "
    print(f"Operation code available {flow_text}(<op> : <class>)")
    print(json.dumps(op_dic, indent=2, default=serialize))

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="flowweave")

    subparsers = parser.add_subparsers(dest="command")
    run_parser = subparsers.add_parser("run", help="Run a flow file")
    run_parser.add_argument("flow_file", help="Path to YAML flow file")
    run_parser.add_argument("-l", "--log", help="Show flow log", action='store_true')
    run_parser.add_argument("-p", "--parallel", help="Run flow parallel", action='store_true')

    info_parser = subparsers.add_parser("info", help="Show info")
    info_parser.add_argument(
        "flow_file",
        nargs="?",
        default=None,
        help="Path to YAML flow file"
    )

    return parser

def main() -> None:
    result = Result.SUCCESS

    colorama.init(autoreset=True)

    parser = build_parser()
    args = parser.parse_args()

    setting_path = get_setting_path(args)

    if args.command == "run":
        if not setting_path:
            parser.error("run requires flow_file")
        results = FlowWeave.run(setting_file=setting_path, parallel=args.parallel, show_log = args.log)
        result = all(x == Result.SUCCESS for x in results)
    elif args.command == "info":
        if args.flow_file:
            show_flow_op(setting_path, args.flow_file)
        else:
            show_available_op()
    else:
        parser.print_help()

    return result