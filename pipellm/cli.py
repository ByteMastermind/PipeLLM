import argparse
import os
import sys
from pathlib import Path

from . import core, server


def parse_var(arg):
    key, sep, value = arg.partition("=")
    if not sep:
        raise core.PipelineError(f"invalid --var {arg}, expected KEY=VALUE")
    if value.startswith("@"):
        try:
            value = Path(value[1:]).read_text()
        except OSError as e:
            raise core.PipelineError(e)
    return key, value


def start(args):
    pipe = core.load(args.file)
    if errors := core.check(pipe):
        raise core.PipelineError("\n".join(f"{args.file}: {e}" for e in errors))
    given = dict(parse_var(v) for v in args.var)
    rows = core.load(args.vars_file) if args.vars_file else [{}]
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise core.PipelineError(f"{args.vars_file}: must be a list of mappings")
    runs = [core.merge_vars(pipe, {**given, **row}) for row in rows]
    for i, vars in enumerate(runs):
        prefix = f"{i + 1}/{len(runs)} " if len(runs) > 1 else ""
        output = core.run(pipe, vars, core.rundir(args.file), lambda m: print(prefix + m, file=sys.stderr, flush=True))
        print(output, flush=True)


def check(args):
    failed = False
    for file in args.files:
        try:
            errors = core.check(core.load(file))
        except core.PipelineError as e:
            errors = [str(e)]
        for e in errors or ["ok"]:
            print(f"{file}: {e}")
        failed |= bool(errors)
    return int(failed)


def main():
    parser = argparse.ArgumentParser(prog="pipellm")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("start", help="run a pipeline")
    p.add_argument("file")
    p.add_argument("--var", action="append", default=[], metavar="KEY=VALUE", help="set a variable, @path reads a file")
    p.add_argument("--vars-file", help="yaml list of variable sets, runs the pipeline once per set")
    p = sub.add_parser("check", help="validate pipelines")
    p.add_argument("files", nargs="+")
    p = sub.add_parser("serve", help="web gui for pipelines in the current directory")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    try:
        sys.exit({"start": start, "check": check, "serve": server.serve}[args.command](args))
    except core.PipelineError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        os._exit(130)
