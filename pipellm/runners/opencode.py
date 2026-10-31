import json
import os
from pathlib import Path

from .base import Events, RunnerError, ask_json, parse_json, tool_line

EFFORTS = None
TOOLS = {
    "Read": "read",
    "Write": "edit",
    "Edit": "edit",
    "Glob": "glob",
    "Grep": "grep",
    "Bash": "bash",
    "WebFetch": "webfetch",
    "WebSearch": "websearch",
    "TodoWrite": "todowrite",
    "Task": "task",
    "Skill": "skill",
}
# permission checks that are not tools
GUARDS = {"external_directory": "allow", "doom_loop": "allow"}


def error_text(error):
    return error.get("message") or error.get("data", {}).get("message") or json.dumps(error)


def run(prompt, model, effort, schema, tools, cwd, log):
    cmd = ["opencode", "run", "--format", "json", "--auto"]
    if model:
        cmd += ["--model", model]
    if effort:
        cmd += ["--variant", effort]
    if cwd:
        cmd += ["--dir", str(Path(cwd).resolve())]
    permission = {"*": "deny", **GUARDS, **{TOOLS[t]: "allow" for t in tools}}
    env = {**os.environ, "OPENCODE_PERMISSION": json.dumps(permission)}
    step, answer, cost, error = [], "", 0, None
    events = Events(cmd, prompt if schema is None else ask_json(prompt, schema), cwd, env)
    for event in events:
        kind, part = event.get("type"), event.get("part", {})
        if kind == "step_start":
            step = []
        elif kind == "text":
            step.append(part.get("text", ""))
            answer = "".join(step)
        elif kind == "tool_use":
            log(tool_line(part.get("tool"), part.get("state", {}).get("input", {})))
        elif kind == "step_finish":
            cost += part.get("cost", 0)
        elif kind == "error":
            error = error_text(event.get("error", {}))
    if error:
        raise RunnerError(error, cost)
    if events.failed:
        raise RunnerError(events.failure, cost)
    if not answer.strip():
        raise RunnerError("no output", cost)
    return answer if schema is None else parse_json(answer, schema, cost), cost
