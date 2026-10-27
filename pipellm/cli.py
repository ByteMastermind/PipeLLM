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
    if args.sequential:
        for job in pipe["jobs"]:
            job["concurrency"] = 1
    given = dict(parse_var(v) for v in args.var)
    names = [job["name"] for job in pipe["jobs"]]
    if args.resume:
        if args.vars_file:
            raise core.PipelineError("--resume cannot be combined with --vars-file")
        outdir = Path(args.resume)
        if not (outdir / "vars.json").exists():
            raise core.PipelineError(f"{outdir}: not a run directory (no vars.json)")
        saved = core.load(outdir / "vars.json")
        vars = core.merge_vars(pipe, {**saved, **given})
        resume = core.resume_at(names, core.load_jobs(outdir, names), args.from_job)
        output = core.run(pipe, vars, outdir, lambda m: print(m, file=sys.stderr, flush=True), resume)
        print(output, flush=True)
        return
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
    p.add_argument("--sequential", action="store_true", help="run foreach items one at a time")
    p.add_argument("--resume", metavar="RUNDIR", help="continue a saved run, reusing its finished jobs")
    p.add_argument("--from", dest="from_job", metavar="JOB", help="with --resume, re-run from this job (default: first unfinished)")
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
