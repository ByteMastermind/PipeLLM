import argparse
import os
import signal
import sys
import threading
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


def stoppable(work):
    # work runs in a thread, so signals only count presses: the first ctrl+c asks,
    # the second stops the run, and a hangup or terminate stops it at once
    presses, error = 0, None

    def press(sig, frame):
        nonlocal presses
        presses += 1 if sig == signal.SIGINT else 2

    def target():
        nonlocal error
        try:
            work()
        except BaseException as e:
            error = e

    for sig in (signal.SIGINT, signal.SIGHUP, signal.SIGTERM):
        signal.signal(sig, press)
    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    asked = False
    while thread.is_alive():
        thread.join(0.2)
        if presses == 1 and not asked:
            asked = True
            print("stop the run? running agents lose their progress, finished jobs and foreach items stay saved. press ctrl+c again to stop", file=sys.stderr, flush=True)
        elif presses > 1 and not core.stopping.is_set():
            print("stopping", file=sys.stderr, flush=True)
            core.stop()
    if error:
        raise error


def start(args):
    pipe = core.load(args.file)
    if errors := core.check(pipe):
        raise core.PipelineError("\n".join(f"{args.file}: {e}" for e in errors))
    if args.sequential:
        for job in pipe["jobs"]:
            job["concurrency"] = 1
    given = dict(parse_var(v) for v in args.var)
    saved = Path(args.resume) if args.resume else None
    if args.from_job and not saved:
        raise core.PipelineError("--from requires --resume")
    if args.from_job and args.from_job not in [job["name"] for job in pipe["jobs"]]:
        raise core.PipelineError(f"no job named {args.from_job}")
    if saved:
        if args.vars_file:
            raise core.PipelineError("--resume cannot be combined with --vars-file")
        if not (saved / "vars.json").exists():
            raise core.PipelineError(f"{saved}: not a run directory (no vars.json)")
        runs = [core.merge_vars(pipe, {**core.read(saved / "vars.json"), **given})]
    else:
        rows = core.load(args.vars_file) if args.vars_file else [{}]
        if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
            raise core.PipelineError(f"{args.vars_file}: must be a list of mappings")
        runs = [core.merge_vars(pipe, {**given, **row}) for row in rows]

    def work():
        for i, vars in enumerate(runs):
            prefix = f"{i + 1}/{len(runs)} " if len(runs) > 1 else ""
            # --resume continues in the saved folder, --from leaves it as it is and saves to a new one
            outdir = saved if saved and not args.from_job else core.rundir(args.file)
            output = core.run(pipe, vars, outdir, lambda m: print(prefix + m, file=sys.stderr, flush=True), saved, args.from_job)
            print(output, flush=True)

    stoppable(work)


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
    p.add_argument("--from", dest="from_job", metavar="JOB", help="with --resume, run again from this job, saving to a new run folder")
    p = sub.add_parser("check", help="validate pipelines")
    p.add_argument("files", nargs="+")
    p = sub.add_parser("serve", help="web gui for pipelines in the current directory")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    # agents run in their own sessions, so they are stopped here when pipellm is interrupted or closed
    for sig in (signal.SIGHUP, signal.SIGTERM):
        signal.signal(sig, signal.default_int_handler)
    try:
        sys.exit({"start": start, "check": check, "serve": server.serve}[args.command](args))
    except core.PipelineError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
    except core.Stopped as e:
        print(f"stopped, finished jobs and foreach items are saved in {e}, continue with: pipellm start {args.file} --resume {e}", file=sys.stderr)
        sys.exit(130)
    except KeyboardInterrupt:
        core.stop()
        os._exit(130)
