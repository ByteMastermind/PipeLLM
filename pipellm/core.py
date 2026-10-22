import json
import re
import subprocess
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import yaml

TEMPLATE = re.compile(r"\{\{\s*([\w.-]+)\s*\}\}")
NAME = re.compile(r"\w[\w-]*")
EFFORTS = ("low", "medium", "high", "xhigh", "max")
PREVIEW = 80
PIPELINE_TYPES = {"model": str, "effort": str, "vars": dict, "output": str, "jobs": list}
JOB_TYPES = {
    "name": str,
    "model": str,
    "effort": str,
    "prompt": str,
    "schema": dict,
    "foreach": str,
    "concurrency": int,
    "retries": int,
    "tools": list,
    "cwd": str,
    "back": dict,
    "stop": dict,
}
BACK_TYPES = {"to": str, "when": str, "max": int}
STOP_TYPES = {"when": str, "reason": str}


class PipelineError(Exception):
    pass


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


def check(pipe):
    if not isinstance(pipe, dict):
        return ["pipeline must be a mapping"]
    errors = type_errors(pipe, PIPELINE_TYPES, "pipeline")
    if isinstance(pipe.get("effort"), str) and pipe["effort"] not in EFFORTS:
        errors.append(f"pipeline: effort must be one of {', '.join(EFFORTS)}")
    vars = pipe["vars"] if isinstance(pipe.get("vars"), dict) else {}
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
        if "prompt" not in job:
            errors.append(f"{where}: prompt is required")
        if isinstance(job.get("concurrency"), int) and job["concurrency"] < 1:
            errors.append(f"{where}: concurrency must be at least 1")
        if isinstance(job.get("retries"), int) and job["retries"] < 0:
            errors.append(f"{where}: retries must not be negative")
        if isinstance(job.get("effort"), str) and job["effort"] not in EFFORTS:
            errors.append(f"{where}: effort must be one of {', '.join(EFFORTS)}")
        if isinstance(job.get("tools"), list) and not all(isinstance(t, str) and t.isidentifier() for t in job["tools"]):
            errors.append(f"{where}: tools must be a list of tool names")
        refs = [r for k in ("prompt", "model", "cwd") if isinstance(job.get(k), str) for r in TEMPLATE.findall(job[k])]
        errors += [f"{where}: unknown reference {r}" for r in unknown_refs(refs, vars, names[:ends[i]], "foreach" in job)]
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


def render(template, ctx):
    # jobs not run yet in back loops render empty
    return TEMPLATE.sub(lambda m: text(resolve(ctx, m[1])) if ran(ctx, m[1]) else "", template)


def tool_calls(event):
    for block in event["message"]["content"] if event.get("type") == "assistant" else []:
        if block["type"] == "tool_use" and block["name"] != "StructuredOutput":
            args = [v for v in block["input"].values() if isinstance(v, str)]
            yield short(f"{block['name']} {args[0] if args else ''}")


def claude(prompt, model, effort, schema, tools, cwd, log):
    cmd = ["claude", "-p", "--output-format", "stream-json", "--verbose", "--no-session-persistence", "--permission-mode", "bypassPermissions", "--tools", ",".join(tools)]
    if model:
        cmd += ["--model", model]
    if effort:
        cmd += ["--effort", effort]
    # wrap non-object schema into {value: ...}
    wrap = schema is not None and schema.get("type") != "object"
    if schema is not None:
        cmd += ["--json-schema", json.dumps({"type": "object", "properties": {"value": schema}, "required": ["value"]} if wrap else schema)]
    result, other = None, []
    try:
        with subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=cwd or None) as proc:
            proc.stdin.write(prompt)
            proc.stdin.close()
            for line in proc.stdout:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    other.append(line)
                    continue
                if event.get("type") == "result":
                    result = event
                for call in tool_calls(event):
                    log(call)
            stderr = proc.stderr.read()
    except OSError as e:
        raise PipelineError(e)
    if result is None:
        raise PipelineError(stderr.strip() or "".join(other).strip() or f"claude exited with {proc.returncode}")
    if result.get("is_error"):
        raise PipelineError(result.get("result") or result.get("subtype"))
    out = result["result"] if schema is None else result.get("structured_output")
    if out is None:
        raise PipelineError("no structured output")
    return out["value"] if wrap else out, result.get("total_cost_usd", 0)


def attempt(pipe, job, ctx, label, log):
    args = (
        render(job["prompt"], ctx),
        render(job.get("model") or pipe.get("model") or "", ctx),
        job.get("effort") or pipe.get("effort") or "",
        job.get("schema"),
        job.get("tools", []),
        render(job.get("cwd", ""), ctx),
    )
    retries = job.get("retries", 0)
    for n in range(retries + 1):
        start = time.time()
        try:
            out, cost = claude(*args, lambda m: log(f"{label}: {m}"))
        except PipelineError as e:
            if n == retries:
                log(f"{label}: failed")
                raise PipelineError(f"{label}: {e}")
            log(f"{label}: retry {n + 1}/{retries}: {short(str(e))}")
            continue
        log(f"{label}: done {time.time() - start:.1f}s ${cost:.4f} | {short(out)}")
        return out, cost


def run_job(pipe, job, ctx, log):
    name = job["name"]
    if "foreach" not in job:
        log(f"{name}: running")
        return attempt(pipe, job, ctx, name, log)
    items = resolve(ctx, job["foreach"])
    if not isinstance(items, list):
        raise PipelineError(f"{name}: foreach {job['foreach']} is not a list")
    log(f"{name}: running {len(items)} items")
    start = time.time()
    ex = ThreadPoolExecutor(job.get("concurrency", 4))
    futures = [ex.submit(attempt, pipe, job, {**ctx, "item": item}, f"{name}[{i}]", log) for i, item in enumerate(items)]
    try:
        for future in as_completed(futures):
            future.result()
    finally:
        ex.shutdown(wait=False, cancel_futures=True)
    results = [f.result() for f in futures]
    cost = sum(c for _, c in results)
    log(f"{name}: done {time.time() - start:.1f}s ${cost:.4f}")
    return [out for out, _ in results], cost


def write(path, value):
    Path(f"{path}.{'txt' if isinstance(value, str) else 'json'}").write_text(text(value))


def rundir(file):
    return Path("runs") / f"{Path(file).stem}-{time.strftime('%Y%m%d-%H%M%S')}"


def run(pipe, vars, outdir, log):
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
        out, cost = run_job(pipe, job, ctx, log)
        ctx["jobs"][name] = out
        passes[name] += 1
        write(outdir / "jobs" / (name if passes[name] == 1 else f"{name}.{passes[name]}"), out)
        total += cost
        stop = job.get("stop")
        if stop and resolve(ctx, stop["when"]):
            reason = render(stop.get("reason", ""), ctx) or stop["when"]
            raise PipelineError(f"{name}: stop: {reason}")
        back = job.get("back")
        if back and backs[name] < back["max"] and ("when" not in back or resolve(ctx, back["when"])):
            backs[name] += 1
            log(f"{name}: back to {back['to']} {backs[name]}/{back['max']}")
            i = names.index(back["to"])
        else:
            i += 1
    output = render(pipe["output"], ctx) if "output" in pipe else text(out)
    write(outdir / "output", output)
    log(f"done {time.time() - start:.1f}s ${total:.4f} {outdir}")
    return output
