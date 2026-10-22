import json
import re
import secrets
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import yaml

from . import core

INDEX = Path(__file__).with_name("index.html")
PIPELINE = re.compile(r"/api/pipelines/([\w.-]+\.ya?ml)")
RUN = re.compile(r"/api/runs/(\w+)")
TOKEN = secrets.token_urlsafe(16)
runs = {}


class Dumper(yaml.SafeDumper):
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)


Dumper.add_representer(str, lambda d, s: d.represent_scalar("tag:yaml.org,2002:str", s, style="|" if "\n" in s else None))


def start(file, vars):
    pipe = core.load(file)
    if errors := core.check(pipe):
        raise core.PipelineError("\n".join(errors))
    vars = core.merge_vars(pipe, vars)
    rid = uuid.uuid4().hex[:8]
    run = runs[rid] = {"status": "running", "log": [], "output": None}

    def work():
        try:
            run["output"] = core.run(pipe, vars, core.rundir(file), run["log"].append)
            run["status"] = "done"
        except Exception as e:
            run["log"].append(f"error: {e}")
            run["status"] = "failed"

    threading.Thread(target=work, daemon=True).start()
    return {"id": rid}


def route(method, path, body):
    if method == "GET" and path == "/api/pipelines":
        return sorted(p.name for p in Path().glob("*.y*ml"))
    if m := PIPELINE.fullmatch(path):
        if method == "GET":
            return core.load(m[1])
        if method == "PUT":
            Path(m[1]).write_text(yaml.dump(body, Dumper=Dumper, sort_keys=False, allow_unicode=True))
            return core.check(body)
    if method == "POST" and path == "/api/runs":
        return start(body["file"], body["vars"])
    if method == "GET" and (m := RUN.fullmatch(path)) and m[1] in runs:
        return runs[m[1]]
    raise LookupError


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.partition("?")[0] == "/":
            return self.reply(200, "text/html", INDEX.read_bytes())
        self.do_POST()

    def do_POST(self):
        if not secrets.compare_digest(self.headers.get("Authorization", ""), f"Bearer {TOKEN}"):
            return self.reply(401, "text/plain", b"invalid token")
        try:
            size = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(size)) if size else None
            self.reply(200, "application/json", json.dumps(route(self.command, self.path.partition("?")[0], body), default=str).encode())
        except core.PipelineError as e:
            self.reply(400, "text/plain", str(e).encode())
        except LookupError:
            self.reply(404, "text/plain", b"not found")

    do_PUT = do_POST

    def reply(self, code, kind, data):
        self.send_response(code)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


def serve(args):
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"http://{args.host}:{args.port}/#{TOKEN}", flush=True)
    httpd.serve_forever()
