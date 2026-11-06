import json
import os
import subprocess
import threading
import time
import urllib.request
from datetime import datetime
from pathlib import Path

from .base import Events, RunnerError, Stopped, stopping, tool_line

EFFORTS = ("low", "medium", "high", "xhigh", "max")
TOOLS = None
USAGE = "https://api.anthropic.com/api/oauth/usage"
# percent of a usage window that holds new agents
LIMITS = {"five_hour": ("5h", 97), "seven_day": ("weekly", 99)}
PROBE = 60
ignore_usage = False
gate = threading.Lock()
probed, usage, warned = -PROBE, {}, False


def tool_calls(event):
    for block in event["message"]["content"] if event.get("type") == "assistant" else []:
        if block["type"] == "tool_use" and block["name"] != "StructuredOutput":
            yield tool_line(block["name"], block["input"])


def fetch():
    file = Path(os.environ.get("CLAUDE_CONFIG_DIR", "~/.claude")).expanduser() / ".credentials.json"
    try:
        creds = file.read_text()
    except FileNotFoundError:
        # macos keychain
        creds = subprocess.run(["security", "find-generic-password", "-s", "Claude Code-credentials", "-w"], capture_output=True, text=True, check=True).stdout
    token = json.loads(creds)["claudeAiOauth"]["accessToken"]
    request = urllib.request.Request(USAGE, headers={"Authorization": f"Bearer {token}", "anthropic-beta": "oauth-2025-04-20", "User-Agent": "pipellm"})
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def reset(window):
    try:
        return datetime.fromisoformat(window["resets_at"].replace("Z", "+00:00")).timestamp()
    except (KeyError, AttributeError, ValueError):
        return None


def limited():
    # latest reset of the windows over their limit
    hits = [(reset(w), name) for key, (name, limit) in LIMITS.items() if ((w := usage.get(key) or {}).get("utilization") or 0) >= limit]
    return max(hits, key=lambda h: h[0] or 0, default=None)


def wait_for_usage(log):
    # probed at most once per PROBE, one agent at a time waits for the reset
    global probed, usage, warned
    with gate:
        while not stopping.is_set():
            if time.time() - probed >= PROBE:
                probed = time.time()
                try:
                    usage = fetch()
                except Exception as e:
                    usage = {}
                    if not warned:
                        warned = True
                        log(f"usage limits not checked: {e}")
            if not (hit := limited()):
                return
            at, name = hit
            if at:
                log(f"waiting until {time.strftime('%H:%M' if at - time.time() < 86400 else '%a %H:%M', time.localtime(at))} {name} limit reset")
            else:
                log(f"waiting for {name} limit reset")
            stopping.wait(max(PROBE, at - time.time()) if at else PROBE)
        raise Stopped


def run(prompt, model, effort, schema, tools, cwd, log):
    if not ignore_usage:
        wait_for_usage(log)
    cmd = ["claude", "-p", "--output-format", "stream-json", "--verbose", "--no-session-persistence", "--permission-mode", "bypassPermissions", "--tools", ",".join(tools)]
    if model:
        cmd += ["--model", model]
    if effort:
        cmd += ["--effort", effort]
    # wrap non-object schema into {value: ...}
    wrap = schema is not None and schema.get("type") != "object"
    if schema is not None:
        cmd += ["--json-schema", json.dumps({"type": "object", "properties": {"value": schema}, "required": ["value"]} if wrap else schema)]
    result = None
    events = Events(cmd, prompt, cwd)
    for event in events:
        if event.get("type") == "result":
            result = event
        for call in tool_calls(event):
            log(call)
    if result is None:
        raise RunnerError(events.failure)
    cost = result.get("total_cost_usd", 0)
    if result.get("is_error"):
        raise RunnerError(result.get("result") or result.get("subtype"), cost)
    out = result["result"] if schema is None else result.get("structured_output")
    if out is None:
        raise RunnerError("no structured output", cost)
    return out["value"] if wrap else out, cost
