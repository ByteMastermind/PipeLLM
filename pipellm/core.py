import contextlib
import json
import os
import re
import shutil
import signal
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import yaml

from .runners import RUNNERS
from .runners.base import RunnerError, Stopped, agents, spawn, stopping

TEMPLATE = re.compile(r"\{\{\s*([\w.-]+)\s*\}\}")
NAME = re.compile(r"\w[\w-]*")
RUNNER = "claude"
PREVIEW = 80
RETRIES = 2
BACKOFF = 5
PIPELINE_TYPES = {"runner": str, "model": str, "effort": str, "vars": dict, "output": str, "jobs": list}
JOB_TYPES = {
    "name": str,
    "runner": str,
    "model": str,
    "effort": str,
    "prompt": str,
    "schema": dict,
    "foreach": str,
    "concurrency": int,
    "retries": int,
    "max_failures": int,
    "tools": list,
    "cwd": str,
    "back": dict,
    "stop": dict,
    "next": list,
    "pipeline": str,
    "vars": dict,
    "on_stop": str,
}
AGENT_KEYS = ("runner", "model", "effort", "schema", "tools", "cwd", "retries")
BACK_TYPES = {"to": str, "when": str, "max": int}
STOP_TYPES = {"when": str, "reason": str}
NEXT_TYPES = {"to": str, "when": str}


class PipelineError(Exception):
    # stop: reason of a stop job
    def __init__(self, message, cost=0, stop=None):
        super().__init__(message)
        self.cost, self.stop = cost, stop


# running foreach jobs, for stop() to end
aborts = set()
# foreach items one at a time
sequential = False


def stop():
    # ends every run in this process: agents are killed, retries wake up, and the jobs
    # that have not finished raise Stopped without being saved
    with spawn:
        stopping.set()
    for abort in list(aborts):
        abort.set()
    for proc in list(agents):
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGTERM)


def load(path):
    try:
        return yaml.safe_load(Path(path).read_text())
    except yaml.MarkedYAMLError as e:
        raise PipelineError(f"line {e.problem_mark.line + 1}: {e.problem}")
    except (OSError, yaml.YAMLError) as e:
        raise PipelineError(e)


def type_errors(obj, types, where):
    errors = [f"{where}: unknown key {k}" for k in obj if k not in types]
    return errors + [f"{where}: {k} must be {t.__name__}" for k, t in types.items() if k in obj and not isinstance(obj[k], t)]


def unknown_refs(refs, vars, jobs, item):
    for ref in refs:
        head, _, rest = ref.partition(".")
        key = rest.partition(".")[0]
        if not (head == "vars" and key in vars or head == "jobs" and key in jobs or head == "item" and item):
            yield ref


def reach(jobs, names):
    # prompts can also reference later jobs of their back loops
    ends = list(range(len(jobs)))
    for j, job in enumerate(jobs):
        back = job.get("back") if isinstance(job, dict) else None
        if isinstance(back, dict) and back.get("to") in names[:j + 1]:
            for i in range(names.index(back["to"]), j + 1):
                ends[i] = j + 1
    return ends


def runner_errors(name, effort, tools):
    runner = RUNNERS.get(name)
    if runner is None:
        return [f"runner must be one of {', '.join(RUNNERS)}"]
    errors = []
    if effort and runner.EFFORTS and effort not in runner.EFFORTS:
        errors.append(f"effort must be one of {', '.join(runner.EFFORTS)} for runner {name}")
    if runner.TOOLS and (unknown := [t for t in tools if t not in runner.TOOLS]):
        errors.append(f"runner {name} does not support tools {', '.join(unknown)}")
    return errors


def child_errors(name, given, path, seen):
    file = Path(path).parent / name
    if file.resolve() in seen:
        return [f"pipeline {name} includes itself"]
    try:
        child = load(file)
    except PipelineError as e:
        return [f"pipeline {name}: {e}"]
    if errors := check(child, file, seen):
        return [f"{name}: {e}" for e in errors]
    defaults = child.get("vars") or {}
    errors = [f"unknown var {k} of pipeline {name}" for k in given if k not in defaults]
    return errors + [f"missing var {k} of pipeline {name}" for k, v in defaults.items() if v is None and k not in given]


def check(pipe, path, seen=()):
    if not isinstance(pipe, dict):
        return ["pipeline must be a mapping"]
    seen = (*seen, Path(path).resolve())
    errors = type_errors(pipe, PIPELINE_TYPES, "pipeline")
    vars =pipe["vars"] if isinstance(pipe.get("vars"), dict) else {}
    jobs = pipe["jobs"] if isinstance(pipe.get("jobs"), list) else []
    if not jobs:
        errors.append("pipeline: jobs must be a non-empty list")
    names = [job.get("name") if isinstance(job, dict) else None for job in jobs]
    ends = reach(jobs, names)
    for i, job in enumerate(jobs):
        if not isinstance(job, dict):
            errors.append(f"jobs[{i}]: must be a mapping")
            continue
        name = job.get("name")
        where = f"job {name or i}"
        errors += type_errors(job, JOB_TYPES, where)
        if not isinstance(name, str) or not NAME.fullmatch(name):
            errors.append(f"{where}: name must match {NAME.pattern}")
        elif name in names[:i]:
            errors.append(f"{where}: duplicate name")
        if ("prompt" in job) == ("pipeline" in job):
            errors.append(f"{where}: needs either prompt or pipeline")
        if "pipeline" in job:
            errors += [f"{where}: {k} does not apply to a pipeline job" for k in AGENT_KEYS if k in job]
        else:
            errors += [f"{where}: {k} requires pipeline" for k in ("vars", "on_stop") if k in job]
        if job.get("on_stop") not in (None, "fail", "continue"):
            errors.append(f"{where}: on_stop must be fail or continue")
        if isinstance(job.get("concurrency"), int) and job["concurrency"] < 1:
            errors.append(f"{where}: concurrency must be at least 1")
        if isinstance(job.get("retries"), int) and job["retries"] < 0:
            errors.append(f"{where}: retries must not be negative")
        if isinstance(job.get("max_failures"), int) and job["max_failures"] < 0:
            errors.append(f"{where}: max_failures must not be negative")
        if "max_failures" in job and "foreach" not in job:
            errors.append(f"{where}: max_failures requires foreach")
        tools = job["tools"] if isinstance(job.get("tools"), list) else []
        if not all(isinstance(t, str) and t.isidentifier() for t in tools):
            errors.append(f"{where}: tools must be a list of tool names")
            tools = []
        runner = job.get("runner") or pipe.get("runner") or RUNNER
        if isinstance(runner, str) and not TEMPLATE.search(runner):
            errors += [f"{where}: {e}" for e in runner_errors(runner, job.get("effort") or pipe.get("effort"), tools)]
        given = job["vars"] if isinstance(job.get("vars"), dict) else {}
        texts = [job.get(k) for k in ("prompt", "runner", "model", "cwd")] + list(given.values())
        refs = [r for t in texts if isinstance(t, str) for r in TEMPLATE.findall(t)]
        errors += [f"{where}: unknown reference {r}" for r in unknown_refs(refs, vars, names[:ends[i]], "foreach" in job)]
        if isinstance(job.get("pipeline"), str):
            errors += [f"{where}: {e}" for e in child_errors(job["pipeline"], given, path, seen)]
        if isinstance(job.get("foreach"), str):
            errors += [f"{where}: unknown foreach {r}" for r in unknown_refs([job["foreach"]], vars, names[:i], False)]
        back = job.get("back")
        if isinstance(back, dict):
            errors += type_errors(back, BACK_TYPES, f"{where}: back")
            if back.get("to") not in names[:i + 1]:
                errors.append(f"{where}: back to must be this or an earlier job")
            if "max" not in back:
                errors.append(f"{where}: back max is required")
            elif isinstance(back["max"], int) and back["max"] < 1:
                errors.append(f"{where}: back max must be at least 1")
            if isinstance(back.get("when"), str):
                errors += [f"{where}: unknown back when {r}" for r in unknown_refs([back["when"]], vars, names[:i + 1], False)]
        stop = job.get("stop")
        if isinstance(stop, dict):
            errors += type_errors(stop, STOP_TYPES, f"{where}: stop")
            if "when" not in stop:
                errors.append(f"{where}: stop when is required")
            elif isinstance(stop["when"], str):
                errors += [f"{where}: unknown stop when {r}" for r in unknown_refs([stop["when"]], vars, names[:i + 1], False)]
            if isinstance(stop.get("reason"), str):
                errors += [f"{where}: unknown stop reason {r}" for r in unknown_refs(TEMPLATE.findall(stop["reason"]), vars, names[:i + 1], False)]
        steps = job.get("next")
        if isinstance(steps, list):
            for n, step in enumerate(steps):
                if not isinstance(step, dict):
                    errors.append(f"{where}: next[{n}] must be a mapping")
                    continue
                errors += type_errors(step, NEXT_TYPES, f"{where}: next[{n}]")
                if step.get("to") not in names[i + 1:]:
                    errors.append(f"{where}: next[{n}] to must be a later job")
                if isinstance(step.get("when"), str):
                    errors += [f"{where}: unknown next[{n}] when {r}" for r in unknown_refs([step["when"]], vars, names[:i + 1], False)]
    if isinstance(pipe.get("output"), str):
        refs = TEMPLATE.findall(pipe["output"])
        errors += [f"pipeline: unknown reference {r}" for r in unknown_refs(refs, vars, names, False)]
    return errors


def merge_vars(pipe, given):
    defaults = pipe.get("vars") or {}
    if unknown := [k for k in given if k not in defaults]:
        raise PipelineError(f"unknown vars: {', '.join(unknown)}")
    vars = {**defaults, **given}
    if missing := [k for k, v in vars.items() if v is None]:
        raise PipelineError(f"missing vars: {', '.join(missing)}")
    return vars


def text(value):
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, indent=2)


def short(value):
    line = " ".join(value.split()) if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    return line if len(line) <= PREVIEW else line[:PREVIEW - 1] + "…"


def resolve(ctx, path):
    value = ctx
    for key in path.split("."):
        try:
            value = value[int(key)] if isinstance(value, list) else value[key]
        except (KeyError, IndexError, ValueError, TypeError):
            raise PipelineError(f"cannot resolve {path}")
    return value


def ran(ctx, path):
    head, _, rest = path.partition(".")
    return head != "jobs" or rest.partition(".")[0] in ctx["jobs"]


def holds(ctx, when):
    return when is None or ran(ctx, when) and resolve(ctx, when)


def render(template, ctx):
    # jobs not run yet or skipped render empty
    return TEMPLATE.sub(lambda m: text(resolve(ctx, m[1])) if ran(ctx, m[1]) else "", template)


def value(template, ctx):
    # a lone reference keeps its value
    m = TEMPLATE.fullmatch(template)
    return resolve(ctx, m[1]) if m and ran(ctx, m[1]) else render(template, ctx)


def attempt(pipe, job, ctx, label, log, abort):
    retries, spent, start = job.get("retries", RETRIES), 0, time.time()
    try:
        runner = render(job.get("runner") or pipe.get("runner") or RUNNER, ctx)
        effort = job.get("effort") or pipe.get("effort") or ""
        tools = job.get("tools", [])
        if errors := runner_errors(runner, effort, tools):
            raise PipelineError("; ".join(errors))
        args = (
            render(job["prompt"], ctx),
            render(job.get("model") or pipe.get("model") or "", ctx),
            effort,
            job.get("schema"),
            tools,
            render(job.get("cwd", ""), ctx),
        )
        for n in range(retries + 1):
            try:
                out, cost = RUNNERS[runner].run(*args, lambda m: log(f"{label}: {short(m)}"))
                break
            except RunnerError as e:
                spent += e.cost
                # back off before retrying, give up early once the job is aborted or the run stopped
                if n == retries or abort.wait(BACKOFF * 2 ** n):
                    raise
                log(f"{label}: retry {n + 1}/{retries}: {short(str(e))}")
    except (PipelineError, RunnerError) as e:
        log(f"{label}: failed: {short(str(e))}")
        raise PipelineError(f"{label}: {e}", spent)
    log(f"{label}: done {time.time() - start:.1f}s ${spent + cost:.4f} | {short(out)}")
    return out, spent + cost


def call(pipe, path, job, ctx, label, log, folder, saved, abort):
    # an agent, or a child pipeline run in folder
    if "prompt" in job:
        return attempt(pipe, job, ctx, label, log, abort)
    try:
        file = Path(path).parent / job["pipeline"]
        child = load(file)
        vars = merge_vars(child, {k: value(v, ctx) if isinstance(v, str) else v for k, v in job.get("vars", {}).items()})
        return run(child, file, vars, folder, lambda m: log(f"{label}: {m}"), saved, abort=abort)
    except PipelineError as e:
        if e.stop and job.get("on_stop") == "continue":
            log(f"{label}: stopped | {short(e.stop)}")
            return e.stop, e.cost
        log(f"{label}: failed: {short(str(e))}")
        raise PipelineError(f"{label}: {e}", e.cost) from None


def run_job(pipe, path, job, ctx, log, folder, saved):
    # foreach items are written to folder as they finish, and reused from saved, a folder of an earlier run
    name = job["name"]
    if "foreach" not in job:
        log(f"{name}: running")
        return call(pipe, path, job, ctx, name, log, folder, saved, stopping)
    items = resolve(ctx, job["foreach"])
    if not isinstance(items, list):
        raise PipelineError(f"{name}: foreach {job['foreach']} is not a list")
    log(f"{name}: running {len(items)} items")
    start = time.time()
    folder.mkdir(exist_ok=True)
    # failed items output null, the job fails once more than max_failures items fail
    limit = job.get("max_failures", len(items) - 1)
    abort, lock, failures = threading.Event(), threading.Lock(), []
    aborts.add(abort)

    def one(i, item):
        # counted on the worker, so it never starts another item once the job has failed
        if abort.is_set():
            raise PipelineError(f"{name}[{i}]: cancelled")
        if saved and (found := find(saved / str(i))):
            out, cost = read(found), 0
            log(f"{name}[{i}]: reused | {short(out)}")
        else:
            try:
                out, cost = call(pipe, path, job, {**ctx, "item": item}, f"{name}[{i}]", log, folder / str(i), saved and saved / str(i), abort)
            except PipelineError as e:
                # once aborted, child runs fail as cancelled
                with lock:
                    if not abort.is_set():
                        failures.append(e)
                    if len(failures) > limit:
                        abort.set()
                raise
        write(folder / str(i), out)
        return out, cost

    outs, cost = [None] * len(items), 0
    ex = ThreadPoolExecutor(1 if sequential else job.get("concurrency", 4))
    futures = {ex.submit(one, i, item): i for i, item in enumerate(items)}
    try:
        for future in as_completed(futures):
            try:
                outs[futures[future]], spent = future.result()
            except PipelineError as e:
                spent = e.cost
            cost += spent
            if abort.is_set():
                raise PipelineError(f"{name}: {len(failures)}/{len(items)} items failed, last {failures[-1]}")
    finally:
        aborts.discard(abort)
        abort.set()
        ex.shutdown(wait=False, cancel_futures=True)
    failed = f", {len(failures)}/{len(items)} items failed" if failures else ""
    log(f"{name}: done {time.time() - start:.1f}s ${cost:.4f}{failed}")
    return outs, cost


def write(path, value):
    Path(f"{path}.{'txt' if isinstance(value, str) else 'json'}").write_text(text(value))


def find(path):
    # the file write() saved for path, None if there is none
    return next((p for p in (Path(f"{path}.txt"), Path(f"{path}.json")) if p.exists()), None)


def read(file):
    return file.read_text() if file.suffix == ".txt" else json.loads(file.read_text())


def rundir(file):
    return Path("runs") / f"{Path(file).stem}-{time.strftime('%Y%m%d-%H%M%S')}"


def run(pipe, path, vars, outdir, log, saved=None, rerun=None, abort=stopping):
    # with saved, a run folder, jobs reuse its outputs for as long as this run retraces it, so back
    # loops and next branches take the same path; from the first job without one (or rerun) on, all jobs run,
    # the first one reusing its finished foreach items and child runs
    start, total = time.time(), 0
    ctx = {"vars": vars, "jobs": {}}
    jobs = pipe["jobs"]
    names = [job["name"] for job in jobs]
    passes, backs = Counter(), Counter()
    (outdir / "jobs").mkdir(parents=True, exist_ok=True)
    write(outdir / "vars", vars)
    i = 0
    while i < len(jobs):
        job, name = jobs[i], names[i]
        passes[name] += 1
        file = name if passes[name] == 1 else f"{name}.{passes[name]}"
        prior = saved / "jobs" / file if saved and name != rerun else None
        if prior and (found := find(prior)):
            out, cost = read(found), 0
            log(f"{name}: reused | {short(out)}")
        else:
            saved = None
            try:
                if abort.is_set():
                    raise PipelineError(f"{name}: cancelled")
                out, cost = run_job(pipe, path, job, ctx, log, outdir / "jobs" / file, prior)
            except (PipelineError, Stopped) as e:
                # whatever a job raises once the run is stopping, it was stopped
                if stopping.is_set():
                    raise Stopped(outdir) from None
                e.cost += total
                raise
        ctx["jobs"][name] = out
        write(outdir / "jobs" / file, out)
        # foreach items, now saved with the job; child runs stay
        if "prompt" in job:
            shutil.rmtree(outdir / "jobs" / file, ignore_errors=True)
        total += cost
        stop = job.get("stop")
        if stop and holds(ctx, stop["when"]):
            reason = render(stop.get("reason", ""), ctx) or stop["when"]
            raise PipelineError(f"{name}: stop: {reason}", total, reason)
        back = job.get("back")
        if back and backs[name] < back["max"] and holds(ctx, back.get("when")):
            backs[name] += 1
            log(f"{name}: back to {back['to']} {backs[name]}/{back['max']}")
            i = names.index(back["to"])
        elif to := next((s["to"] for s in job.get("next", []) if holds(ctx, s.get("when"))), None):
            log(f"{name}: next {to}")
            for skipped in names[i + 1:names.index(to)]:
                ctx["jobs"].pop(skipped, None)
            i = names.index(to)
        else:
            i += 1
    output = value(pipe["output"], ctx) if "output" in pipe else out
    write(outdir / "output", output)
    log(f"done {time.time() - start:.1f}s ${total:.4f} {outdir}")
    return output, total
