# pipellm

Run pipelines of Claude Code agents defined in YAML.

## Install

```
pipx install -e .
```

## Usage

```
pipellm check article.yaml
pipellm start article.yaml --var topic="sqlite WAL mode" > article.md
pipellm start article.yaml --vars-file topics.yaml
pipellm start notes.yaml --var doc=@notes.txt
pipellm serve
```

`start` prints progress to stderr (job states, agent tool calls, a one-line preview of each output) and the final output to stdout. Every run is saved to `runs/<pipeline>-<timestamp>/` (`vars.json`, `jobs/<name>.txt|json`, `output.txt`); jobs run again by `back` are saved as `jobs/<name>.2.txt|json` and so on.

Agents run with `--permission-mode bypassPermissions`, so they use their `tools` without asking and `Bash` can run any command.

`--vars-file` is a YAML list of variable sets; the pipeline runs once per set.

`serve` opens a web editor for the pipelines in the current directory. It prints a URL with an access token; anyone with the URL can run agents on this machine, so keep the default `--host 127.0.0.1` unless you trust the network.

## Pipeline

```yaml
model: sonnet              # default model for all jobs
effort: high               # default effort for all jobs (low|medium|high|xhigh|max)
vars:
  topic: null              # null means required
  audience: developers
jobs:
  - name: outline
    prompt: Plan an article about {{ vars.topic }}. Return section titles.
    schema:                # json schema, output becomes json instead of text
      type: array
      items: {type: string}
  - name: sections
    model: haiku
    effort: low            # per-job, overrides the default
    foreach: jobs.outline  # one parallel agent per item
    concurrency: 3         # default 4
    retries: 1             # default 0
    prompt: Write the section {{ item }} for {{ vars.audience }}.
  - name: article
    tools: [Read, Bash]    # built-in tools, default none
    cwd: ../repo           # default current directory
    prompt: |
      Join these sections: {{ jobs.sections }}
      Reviewer notes on your previous version: {{ jobs.review.notes }}
  - name: review
    prompt: "Review this article: {{ jobs.article }}"
    schema:
      type: object
      properties: {revise: {type: boolean}, notes: {type: string}}
      required: [revise, notes]
    back:                  # go back and run again from an earlier job
      to: article          # this or an earlier job
      when: jobs.review.revise   # go back if truthy, default always
      max: 2               # required, the last run continues regardless
output: "{{ jobs.article }}"           # default last job output
```

Templates: `{{ vars.x }}`, `{{ jobs.name }}`, `{{ item }}` (inside `foreach` jobs), with nested access like `{{ jobs.outline.0 }}` or `{{ item.title }}`. Non-text values are inserted as JSON. `prompt`, `model` and `cwd` are templated, so `model: "{{ vars.model }}"` lets you choose the model per run.

Inside a `back` loop, prompts can also reference the job itself and later jobs of the loop, like `{{ jobs.review.notes }}` in `article` above. They hold the output of the previous pass and are empty on the first one.

`stop` ends the whole run as soon as the job finishes, if its `when` reference is truthy, printing `reason` (a template, default the `when` path) and exiting non-zero. Use it to bail out when a prerequisite is missing:

```yaml
  - name: login
    tools: [Bash]
    prompt: "Run `gh auth status`. Set blocked true if it is not authenticated."
    schema:
      type: object
      properties: {blocked: {type: boolean}, reason: {type: string}}
      required: [blocked]
    stop:
      when: jobs.login.blocked          # stops when truthy
      reason: "{{ jobs.login.reason }}" # default: the when path
```

`next` jumps forward to a later job, skipping the jobs in between. Its entries are tried in order, the first whose `when` reference is truthy (default always) wins; if none does, the run continues with the following job. Skipped jobs render empty, so a job after the branches can reference all of them (see `examples/triage.yaml`):

```yaml
  - name: triage
    prompt: "Classify this issue: {{ vars.issue }}"
    schema:
      type: object
      properties: {bug: {type: boolean}, feature: {type: boolean}}
      required: [bug, feature]
    next:
      - to: diagnose              # if
        when: jobs.triage.bug
      - to: design                # else if
        when: jobs.triage.feature
      - to: answer                # else
  - name: diagnose
    prompt: "Diagnose this bug: {{ vars.issue }}"
    next: [{to: reply}]           # skip the other branches
  - name: design
    prompt: "Design this feature: {{ vars.issue }}"
    next: [{to: reply}]
  - name: answer
    prompt: "Answer this question: {{ vars.issue }}"
  - name: reply
    prompt: "Reply to the author: {{ jobs.diagnose }}{{ jobs.design }}{{ jobs.answer }}"
```
