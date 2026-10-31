import json
import re
import subprocess
import threading

import jsonschema

FENCE = re.compile(r"```(?:json)?\s*\n(.*?)```", re.S)

# running agents, for stop() to kill
agents, spawn = set(), threading.Lock()
stopping = threading.Event()


class RunnerError(Exception):
    def __init__(self, message, cost=0):
        super().__init__(message)
        self.cost = cost


class Stopped(Exception):
    # not a RunnerError, so it skips retries and foreach failure counting
    pass


class Events:
    # json lines of an agent cli
    def __init__(self, cmd, prompt, cwd, env=None):
        self.cmd, self.prompt, self.cwd, self.env = cmd, prompt, cwd, env
        self.failed, self.failure = False, None

    def __iter__(self):
        other, proc = [], None
        try:
            with spawn:
                if stopping.is_set():
                    raise Stopped
                # in its own session, so ctrl+c reaches only pipellm and stop() kills the agent with its tools
                proc = subprocess.Popen(self.cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=self.cwd or None, env=self.env, start_new_session=True)
                agents.add(proc)
            with proc:
                proc.stdin.write(self.prompt)
                proc.stdin.close()
                for line in proc.stdout:
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        other.append(line)
                        continue
                    yield event
                stderr = proc.stderr.read()
        except OSError as e:
            raise RunnerError(e)
        finally:
            agents.discard(proc)
        if stopping.is_set():
            raise Stopped
        self.failed = proc.returncode != 0
        self.failure = stderr.strip() or "".join(other).strip() or f"{self.cmd[0]} exited with {proc.returncode}"


def tool_line(name, input):
    args = [v for v in input.values() if isinstance(v, str)]
    return f"{name} {args[0] if args else ''}"


def ask_json(prompt, schema):
    return f"{prompt}\n\nReply with only a JSON value matching this JSON schema, without any other text:\n{json.dumps(schema)}"


def parse_json(text, schema, cost):
    fence = FENCE.search(text)
    try:
        value = json.loads(fence[1] if fence else text)
        jsonschema.validate(value, schema)
    except json.JSONDecodeError as e:
        raise RunnerError(f"output is not json: {e}", cost)
    except jsonschema.ValidationError as e:
        raise RunnerError(f"output does not match schema: {e.message}", cost)
    except jsonschema.SchemaError as e:
        raise RunnerError(f"invalid schema: {e.message}", cost)
    return value
