import argparse
import json
import os
from pathlib import Path
from datetime import date


def date_converter(o):
    if isinstance(o, date):
        return o.__str__()


def save_json(full_json, dir_path, file_name):
    Path(dir_path).mkdir(parents=True, exist_ok=True)
    file_path = os.path.join(dir_path, file_name)

    with open(file_path, 'w') as outfile:
        json.dump(full_json, outfile, default=date_converter, indent=4)


def parse_argv():
    p = argparse.ArgumentParser()
    p.add_argument('--count')
    p.add_argument('--range')
    p.add_argument('--folder')
    p.add_argument('--filename')
    return p.parse_args()


def get_args():
    args = parse_argv()
    return args.count or 1000, args.range or (1, 5), args.folder or "./data", args.filename or "log.json"
