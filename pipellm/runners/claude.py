import json

from .base import Events, RunnerError, tool_line

EFFORTS = ("low", "medium", "high", "xhigh", "max")
TOOLS = None


def tool_calls(event):
    for block in event["message"]["content"] if event.get("type") == "assistant" else []:
        if block["type"] == "tool_use" and block["name"] != "StructuredOutput":
            yield tool_line(block["name"], block["input"])


def run(prompt, model, effort, schema, tools, cwd, log):
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
