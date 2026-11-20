# AGENTS.md

Guidance for coding agents working in this repository.

## What this repo is

The `run-detection` service sits in the ISIS Flexible Interactive Automation (FIA) data reduction
pipeline:
1. **Consume**: Polls RabbitMQ quorum queues (`INGRESS_QUEUE_NAME` and `FAILURE_QUEUE_NAME`) for
   messages containing paths to newly created instrument NeXus (`.nxs`) files.
2. **Ingest**: Reads common and instrument-specific metadata from the NeXus/HDF5 file into a
   `JobRequest` instance (`rundetection/ingestion/`).
3. **Verify & Enrich**: Fetches the remote instrument specification from the FIA API
   (`GET /instrument/{INSTRUMENT}/specification`), builds `Rule` instances via `rule_factory`, and
   verifies/enriches the `JobRequest` (`rundetection/specifications.py`, `rundetection/rules/`).
4. **Publish**: Publishes verified `JobRequest` JSON payloads (plus any `additional_requests`, such
   as stitched runs) to `EGRESS_QUEUE_NAME`, or routes transient failures to `FAILURE_QUEUE_NAME`.

See `README.md` for configuration, container commands, and worked examples of adding custom NeXus
extraction functions and specification rules — read it before making changes.

## Position in the FIA service

### Overarching FIA architecture

FIA is a multi-repository system (all checked out as siblings under the same parent workspace
directory) that automatically reduces instrument data, and also lets users browse, plot, and rerun
that data interactively:

```text
[ISIS Instrument Archive (/archive, /imat) & lastrun.txt]
        │
        ▼
  file-watcher (Kopf Operator + Per-Instrument Watcher Pods)
        │  publishes file path to RabbitMQ: `watched-files`
        ▼
  run-detection (THIS REPO) ◄──── fetches instrument rules (`GET /instrument/{inst}/specification`) ────► FIA-API
        │  ingests .nxs metadata, verifies rules, publishes JobRequest JSON
        │  to RabbitMQ: `scheduled-jobs` (or retries via `failed-watched-files`)
        ▼
  jobcontroller (job_creator) ◄──── requests PreScript (`GET /script/{inst}`) & posts Job ──► FIA-API ◄──► autoreduction-scripts
        │  spawns Kubernetes Job in cluster:
        ├──► Main container: runners (`ghcr.io/fiaisis/mantid` or `mantid-imaging`) -> writes reduced data to `/ceph`
        └──► Sidecar container: jobcontroller (job_watcher) -> updates job status/outputs in FIA-API (`PATCH /job/{id}`)

[Interactive / Web & Live Data Paths]
  scigateway (Host App) ──► frontend (FIA React Plugin)
                                ├──► fia-auth (UOWS login, JWT verify/refresh, `/experiments` RB lookup)
                                ├──► FIA-API (runs, jobs, scripts, specifications, reruns -> `watched-files` / `scheduled-jobs`)
                                └──► plotting-service (h5grove NeXus/HDF5 & IMAT image serving from `/ceph` & `/imat`)

  Kafka (`<INST>_events`, `<INST>_sampleEnv`, `<INST>_runInfo`)
        └──► live-data (Live Data Operator + Mantid Live Data Processor fetching `/live-data/{inst}/script` from FIA-API)

  LLSP (Long Lived Script Processors: `LLSP-API` + RabbitMQ + Celery `LLSP-Worker` built on `ghcr.io/fiaisis/mantid`)

[Infrastructure & Deployment]
  k8s (Terraform + Ansible for OpenStack clusters, Redpanda, local Minikube)
  gitops (ArgoCD App-of-Apps & Kustomize `base`/`envs`/`variants` manifests for all FIA services)
```

### This repository's location (upstream, downstream & siblings)

- **Role**: `run-detection` is the gatekeeper and metadata enrichment engine between raw archive
  file events and actual reduction job creation.
- **Upstream inputs (what triggers `run-detection`)**:
  - RabbitMQ `watched-files` queue (`INGRESS_QUEUE_NAME`) — receives `.nxs` file paths published by
    `file-watcher` (`../file-watcher`) when new runs appear on `/archive`, or by `FIA-API`
    (`../FIA-API`) when a user resubmits a run to watched-files.
  - RabbitMQ `failed-watched-files` queue (`FAILURE_QUEUE_NAME`) — receives `.nxs` file paths
    re-queued by `run-detection` itself after transient failures.
  - `/archive`, `/imat`, and `/ngem/nGEM-INES` mounts — read directly to ingest NeXus HDF5 datasets,
    sibling runs, and XML journal files.
- **Downstream outputs (what `run-detection` calls or publishes to)**:
  - `FIA-API` (`../FIA-API`) — calls `GET /instrument/{INSTRUMENT}/specification` (authenticated
    with `FIA_API_API_KEY`) to retrieve the active rule dictionary for each instrument.
  - RabbitMQ `scheduled-jobs` queue (`EGRESS_QUEUE_NAME`) — publishes verified `JobRequest` JSON
    payloads consumed by `jobcontroller`'s `job_creator` (`../jobcontroller`).
- **Sibling repositories to check when changing contracts**: `../file-watcher` (incoming file path
  format), `../FIA-API` (specification endpoint and `additional_values` keys expected by instrument
  transforms in `fia_api/scripts/transforms/`), `../jobcontroller` (`JobRequest` JSON schema consumed
  by `job_creator`), and `../gitops` (`components/rundetection/` deployment manifests and env vars).

### Core technologies
- **Python**: version/minimum is defined by `requires-python` in `pyproject.toml` — do not hardcode a
  version here; always check `pyproject.toml` (and `.github/workflows/`, which pins the CI version)
  before creating an environment.
- **pika**: RabbitMQ blocking client (`BlockingConnection` / `BlockingChannel` with quorum queues).
- **h5py**: NeXus/HDF5 file access (no type stubs; imported with `# type: ignore`).
- **xmltodict**: XML parsing for ISIS instrument journal files.
- **requests**: HTTP client for fetching specifications from the FIA API.
- **pytest**: Unit and E2E testing (`pytest-cov`, `pytest-random-order`).
- **ruff**: Linting and formatting (configured in `pyproject.toml`).
- **mypy**: Static type checking (`mypy --strict rundetection`).
- **Docker**: Containerization (`container/Dockerfile`).

---

## Repository structure

```
rundetection/                Main application package — CLI entrypoint, RabbitMQ polling/publishing
                              loop, JobRequest dataclass, InstrumentSpecification fetcher/verifier,
                              liveness heartbeat thread, and custom exception hierarchy at top level
  ingestion/                 NeXus (.nxs) HDF5 loading, sibling file/run helpers, and per-instrument
                              metadata extraction functions + dispatcher
  rules/                     Verification rule engine: abstract Rule[T] base class, rule_factory
                              dispatcher, shared rules/journal helpers, and one rules module per
                              instrument (enginx, gem, imat, inter, iris, mari, osiris, sans, vesuvio)

test/                        Pytest test suite mirroring rundetection/
  ingestion/, rules/         Unit tests per module and instrument
  e2e_components/            Fake FIA API and Docker Compose setup used by the E2E test suite
  test_data/                 Sample .nxs files, instrument journals, and per-instrument specification
                              JSON fixtures

container/                   Production Dockerfile and container liveness probe script
tools/                       Developer utility scripts for generating and migrating specifications
.github/workflows/           CI workflows: unit/e2e tests, formatting & linting, container build/push
pyproject.toml               Single source of truth for package metadata, pinned dependencies, and
                              ruff/mypy/pytest configuration
```

---

## `pyproject.toml` — read this first

`pyproject.toml` is the single source of truth for dependencies, extras, and tool config. Read it
before adding dependencies, changing supported Python versions, or modifying lint/type rules:

- Runtime dependencies and optional extras (`formatting`, `test`, `dev`, `all`) are pinned with `==`
  — keep any new dependencies pinned the same way.
- `requires-python` defines the minimum supported Python version; check it and `.github/workflows/`
  before creating a local environment.
- Ruff (`[tool.ruff]`, `[tool.ruff.lint]`) enforces `line-length = 120` and a broad rule set
  (docstrings `D`, pathlib `PTH`, type-checking imports `TC`/`TCH`, no print `T20`, no commented-out
  code `ERA`, etc.). Check `[tool.ruff.lint.ignore]` before "fixing" intentionally ignored rules.
- CI runs `mypy --strict rundetection`.

---

## Architecture & domain conventions

### 1. Exception & queue routing semantics
Understanding how exceptions are handled in `rundetection` is critical when writing extractors or
rules:

| Outcome / Exception | Where Raised | Behavior |
| :--- | :--- | :--- |
| **`RuleViolationError`** (or setting `job_request.will_reduce = False`) | Inside `Rule.verify()` | Caught in `InstrumentSpecification.verify()`; sets `job_request.will_reduce = False` and stops evaluating remaining rules. Message is **acked** and **not** sent to egress or failure queues. |
| **`ReductionMetadataError`** (subclass of `IngestError`) | Inside `ingestion/` or `rules/` | Caught in `process_messages()` (for ingress messages); logged as unrecoverable metadata error, message is **acked** and **dropped** (never sent to `FAILURE_QUEUE_NAME`). |
| **`InterruptedError`** | Anywhere during message processing | Message is **nacked** on the RabbitMQ channel so it remains available, and the exception is re-raised. |
| **Any other `Exception`** (e.g., `IngestError`, `FileNotFoundError`, `requests.HTTPError`, `KeyError`) | Anywhere during ingest/spec fetch/verify | Message is **acked** on the source channel and its decoded path is pushed to `FAILURE_QUEUE_NAME` for retry on subsequent loop iterations. |

### 2. Rule engine & ordering internals
- **`Rule[T]` Contract**:
  - Subclasses inherit from `Rule[T]` where `T` is the type of `self._value` parsed from the
    instrument specification JSON.
  - Implement `verify(self, job_request: JobRequest) -> None`. Rules can mutate
    `job_request.additional_values`, set `job_request.will_reduce = False` (or raise
    `RuleViolationError`), or append derived `JobRequest` objects to
    `job_request.additional_requests`.
- **Rule Ordering (`should_be_first` / `should_be_last`)**:
  - In `Rule.__init__`, `self.should_be_first = False` and `self.should_be_last = False` by default.
  - `InstrumentSpecification._order_rules()` moves rules with `should_be_first = True` to index `0`
    and rules with `should_be_last = True` to the end.
  - **Important**: Any rule that clones `job_request` into `job_request.additional_requests` (such as
    stitch rules like `MolSpecStitchRule`, `MariStitchRule`, `InterStitchRule`) **must** set
    `self.should_be_last = True` so `deepcopy(job_request)` captures all `additional_values`
    populated by earlier rules.
- **Serialization (`JobRequest.to_json_string()`)**:
  - `will_reduce` and `additional_requests` are stripped when serializing a `JobRequest` to JSON;
    `filepath` is converted from `Path` to `str`.
  - When `run.will_reduce` is `True`, `process_message()` enqueues `run` followed by every item in
    `run.additional_requests` onto the notification queue.

### 3. Archive & filesystem path conventions
- Always use `pathlib.Path` (enforced by Ruff `PTH` rules; do not use `os.path`).
- **Standard Archive Layout**:
  - Mounted at `/archive` in production/containers (Linux-style mount path; do not assume `/archive`
    exists on a developer machine).
  - Typical NeXus path:
    `/archive/NDX<INSTRUMENT>/Instrument/data/cycle_<YY>_<N>/<INST_PREFIX><RUN_NUMBER>.nxs` (note:
    TOSCA uses `TSC<run>.nxs` without zero-padding; most instruments use 8-digit zero-padded numbers
    or similar).
  - Typical Journal XML path:
    `/archive/NDX<INSTRUMENT>/Instrument/logs/journal/journal_<YY>_<N>.xml` (i.e.,
    `filepath.parent.parent.parent / f"logs/journal/journal_{cycle_year}_{cycle_num}.xml"`).
- **Cycle String Extraction**:
  - Use `get_cycle_string_from_path(nexus_path)` in `rundetection/ingestion/` to extract
    `cycle_<YY>_<N>` via regex (`r"cycle_(\d+)_(\d+)"`), which raises `IngestError` if not found.
- **Journal Reading & In-Memory Caching**:
  - `run-detection` does **not** use an external cache (such as Valkey/Redis) for journals; instead,
    journal access and caching are handled in-process in three ways depending on the instrument:
    1. **ENGINX (`rundetection/rules/enginx_rules.py`)**: `build_enginx_run_number_cycle_map()` scans
       every `/archive/NDXENGINX/Instrument/logs/journal/*.xml` file to build a `dict[int, str]`
       mapping run numbers to cycle strings (e.g. `242666 -> "15_1"`) and caches it in memory with
       `@lru_cache(maxsize=1)`. `rundetection/run_detection.py` (`main()`) pre-warms this cache at
       startup before starting the RabbitMQ consumer loop.
    2. **SANS (`rundetection/rules/sans_rules.py`)**: Downloads the cycle journal over HTTP
       (`http://data.isis.rl.ac.uk/journals/ndx{instrument}/journal_{cycle_year}_{cycle_num}.xml`),
       parses it into a module-level `SANS_FILES` list, and only re-fetches via
       `_refresh_local_journal(job_request)` when a required run/transmission file is not found in
       the cached list.
    3. **Direct archive journal reads (`rundetection/rules/common_rules.py`)**:
       `get_journal_from_file_based_on_run_file_archive_path()` reads
       `/archive/NDX<INSTRUMENT>/Instrument/logs/journal/journal_<YY>_<N>.xml` directly from disk
       without caching (used by e.g. `mari_rules.py`).
- **IMAT Special Directories**:
  - `IMAT_DIR` (default `/imat`): Base path for experiment directories
    (`/imat/RB<experiment_number>`), searched recursively for `Tomo` directories.
  - `IMAT_NGEM_DIR` (default `/ngem/nGEM-INES`): Fallback nGEM data path
    (`/ngem/nGEM-INES/DATA/IMAT_20<YY>_0<N>/IMAT<run_number:08d>`), using `/raw_data_1/run_cycle`
    from the NeXus dataset.

### 4. Adding features: step-by-step checklists
- **Adding a new instrument extraction function**:
  1. Define `<instrument>_extract(job_request: JobRequest, dataset: Any) -> JobRequest` in
     `rundetection/ingestion/`.
  2. Populate `job_request.additional_values[...]` (converting NumPy/h5py scalar types like
     `np.int32`, `np.float64`, or `bytes` to native Python `int`, `float`, `bool`, `str` so
     `json.dumps` succeeds).
  3. Register the function in `get_extraction_function(instrument: str)`.
  4. Add unit tests under `test/ingestion/`.
- **Adding a new verification rule**:
  1. Implement a `Rule[T]` subclass in the appropriate module under `rundetection/rules/`.
  2. Register the lowercase specification key and expected value type check in `rule_factory()`.
     Raise `ValueError(f"Bad value: {value} in rule: {key_}")` on type mismatch or `MissingRuleError`
     for unknown keys.
  3. If the rule creates `additional_requests` (e.g., stitching), set `self.should_be_last = True` in
     `__init__`.
  4. Update the instrument's test specification JSON under `test/test_data/specifications/` if
     appropriate, and add unit tests under `test/rules/`.

---

## Configuration (environment variables)

| Variable | Default | Description |
| :--- | :--- | :--- |
| `QUEUE_HOST` | `localhost` | RabbitMQ hostname |
| `QUEUE_USER` | `guest` | RabbitMQ username |
| `QUEUE_PASSWORD` | `guest` | RabbitMQ password |
| `INGRESS_QUEUE_NAME` | `watched-files` | Quorum queue consumed for incoming file paths |
| `EGRESS_QUEUE_NAME` | `scheduled-jobs` | Quorum queue where verified `JobRequest` JSONs are published |
| `FAILURE_QUEUE_NAME` | `failed-watched-files` | Quorum queue for retrying runs that failed with transient errors |
| `FIA_API_URL` | `http://localhost:8000` | Base URL for fetching instrument specifications |
| `FIA_API_API_KEY` | *(required at runtime)* | Bearer token for `FIA_API_URL` requests (`os.environ["FIA_API_API_KEY"]`) |
| `IMAT_DIR` | `/imat` | Base directory for IMAT experiment data |
| `IMAT_NGEM_DIR` | `/ngem/nGEM-INES` | Base directory for IMAT nGEM detector data |

---

## Environment & running commands

Do not assume any particular machine already has a conda env, kubectl context, or virtualenv set up
— check what is available first. Prefer `conda` when available (`command -v conda` on Linux/macOS or
`where conda` on Windows); fall back to a Python `venv` only if conda is not installed.

1. Check existing conda environments:
   ```shell
   conda env list
   ```
   If a matching env exists (e.g. `run-detection`), use it with `conda run -n <env-name> <command>`
   or `conda activate <env-name>`.
2. If no suitable conda env exists but `conda` is installed, create one using the Python version
   specified in `pyproject.toml` / `.github/workflows/`:
   ```shell
   conda create -n run-detection python=<version-from-pyproject.toml-or-CI> -y
   conda run -n run-detection pip install ".[all]"
   ```
3. Only if `conda` is not installed, fall back to a virtualenv:
   ```shell
   python3 -m venv .venv        # Linux/macOS (use `python` on Windows)
   . .venv/bin/activate         # POSIX shell (.venv\Scripts\Activate.ps1 on PowerShell)
   pip install ".[all]"
   ```

### Running the service & tests
- **Run CLI locally**:
  ```shell
  conda run -n <env-name> run-detection
  ```
- **Unit tests, linting & type checking (default for all changes)**:
  ```shell
  conda run -n <env-name> pytest . --ignore test/test_e2e.py
  conda run -n <env-name> ruff check .
  conda run -n <env-name> ruff format --check .
  conda run -n <env-name> mypy --strict rundetection
  ```
- **E2E tests (on demand)**: Only run the Docker Compose E2E suite (`test/test_e2e.py`) when
  explicitly asked, or when modifying RabbitMQ queue wiring, container config, or E2E fixtures:
  ```shell
  cd test
  docker compose up -d --build
  cd ..
  conda run -n <env-name> pytest test/test_e2e.py
  ```
  *(Any code changes to `rundetection/` require rebuilding the service container via
  `docker compose up -d --build` inside `test/` before re-running `test_e2e.py`.)*

## Kubernetes commands

Available kubectl contexts vary per machine/operator — always run `kubectl config get-contexts`
first before targeting a cluster. Never rely on the current context; always pass `--context`
explicitly at the **end** of the command:

```shell
kubectl get pods -n fia --context <context-name>
kubectl logs deploy/rundetection -n fia --context <context-name>
```

Double- and triple-check the context name before running anything against a production-like cluster.

## Managing issues

This repo has no local `.github/ISSUE_TEMPLATE/`; GitHub serves the org-wide default template from
`fiaisis/.github` instead. Always use it rather than inventing an ad hoc issue format:

```shell
gh issue create --template Issue
```

Check `gh issue list` / `gh issue view <number>` first to avoid duplicates.

## Branch naming

Branches use `snake_case` descriptions of the work being done. Do not use category prefixes like
`feature/`, `fix/`, or `chore/`.

- **Format**: `description_of_work` (e.g. `imat_rules_update`, `update_agent_guidelines`).

## Commit messages

`main`'s history (`git log --oneline main`) is almost entirely **squash merges**: one commit per PR,
titled `<PR title> (#<PR number>)`, e.g. `Fix queue starvation and consumer leak in process_messages
(#558)`. This has two practical consequences:

- **The PR title is what actually survives into permanent history.** Individual commits on a working
  branch are squashed on merge, so getting the PR title right (see "Pull request process" below)
  matters more than crafting a commit-by-commit history.
- **Individual commit messages still matter for review and for the squash commit body.** By default,
  GitHub's squash merge concatenates every commit subject on the branch into the squashed commit's
  body. Write each commit as a focused, reviewable unit of work with a clear, imperative-mood
  summary line (and a short *why* body paragraph on non-trivial commits) — avoid noise like `wip` or
  `fix` that would leak into the squashed body. Leave auto-generated Dependabot commit messages as-is.

## Requirements for code changes

Only run tests, linters, and type checkers when they are **relevant to the files you actually
changed** — if a change only touches documentation (e.g. `.md` files), skip `pytest`, `ruff`, and
`mypy`. When you do change Python code, dependency manifests, or configuration, before considering
the change complete:
1. **Add or update unit tests** in the matching `test/` subdirectory (e.g. `test/rules/`,
   `test/ingestion/`) for any new or changed behavior.
2. **Add or update the e2e suite** (`test/test_e2e.py`) when the change affects RabbitMQ queue
   wiring, container configuration, or other behavior that suite already exercises.
3. **Run the linter, formatter, and type checker** (`ruff check .`, `ruff format .`,
   `mypy --strict rundetection`) and fix everything reported — don't just rely on CI's auto-fix
   commit.
4. **Run the full unit test suite with coverage** and confirm coverage has not regressed compared
   to `main` (CI uploads results to Codecov; locally, add `--cov-report=term-missing` to the
   `pytest` command in "Testing Standards" to see uncovered lines before and after your change).

## Pull request process

1. **Local validation** before opening a PR (when `.py` / `pyproject.toml` / container files are
   changed; skip for `.md`-only changes):
   ```shell
   conda run -n <env-name> pytest . --ignore test/test_e2e.py
   conda run -n <env-name> ruff check .
   conda run -n <env-name> ruff format .
   conda run -n <env-name> mypy --strict rundetection
   ```
   Note: the CI workflow (`.github/workflows/formatting_and_linting.yml`) also runs `ruff format .`,
   `ruff check --fix`, and `mypy --strict rundetection` on every non-`main` push and auto-commits
   any fixes back to the branch as `"Formatting and linting commit"`.
2. **PR title**: a clear, concise description in normal grammar/sentence case (e.g. `Fix queue
   starvation and consumer leak in process_messages`, `Initial pass on imat rules`). Do not use
   `snake_case` for PR titles.
3. **PR description**: use the org-wide default template from `fiaisis/.github`
   (`.github/pull_request_template.md` — no local override in this repo), which is just
   `Closes # <Issue Number>` followed by a `## Description` section. Ask the user for the issue
   number if one isn't evident from the task, or confirm there is none to link.
4. **AI-assistance label**: attach a label to the PR indicating that it was made with the assistance
   of AI. Check existing labels first with `gh label list` (e.g. `AI` or `AI_code` in this org) and
   apply the matching label via `gh pr create --label <label>` or
   `gh pr edit <number> --add-label <label>`; if the repo does not have an AI label yet, create one
   first (`gh label create "AI" --description "Created with the assistance of AI"`).

## Handling Dependabot PRs

Dependabot opens grouped PRs (`python-packages`, `docker/container`, and
`github_actions/action-packages`). When asked to handle these:

1. **List and identify**: `gh pr list` to find open Dependabot PRs.
2. **Review changes**: `gh pr diff <number>`, paying attention to major version bumps in
   `pyproject.toml`, `container/Dockerfile`, or `.github/workflows/`.
3. **Validate**:
   - Python/dependency changes: `gh pr checkout <number>`, install updated dependencies
     (`pip install ".[all]"`), and run unit tests, ruff, and mypy as in the PR process above.
   - Docker/Action changes: verify CI checks pass with `gh pr checks <number>`.
4. **Approve and merge** once validation passes:
   ```shell
   gh pr review <number> --approve
   gh pr merge <number> --merge
   ```
   Use `--admin` if blocked by branch protection (`gh pr merge <number> --merge --admin`).
5. **Clean up**: close redundant/superseded PRs with `gh pr close <number> --delete-branch`.
6. **Final verification**: checkout `main`, pull latest changes, and re-run
   `pytest . --ignore test/test_e2e.py` to confirm `main` is green.

## General working practices

- Prefer the `read` tool (with `offset`/`limit`) over `sed -n`, `cat`, or similar shell one-liners
  when inspecting file contents, and prefer the `write` and `edit` tools over `bash` (`cat <<EOF`,
  `sed -i`, `echo >`), Python one-liners, or other non-tool-based commands whenever creating or
  modifying files.
- Ask the user a clarifying question whenever a task is ambiguous (e.g. which instrument a rule or
  extractor applies to, how a missing metadata field should be handled, or whether an issue number
  exists to link in a PR) rather than guessing.
- Keep dependency versions pinned with `==` in `pyproject.toml` consistent with the existing style.

## Keeping this document up to date

If a change you make would make any information in this `AGENTS.md` inaccurate or incomplete —
e.g. a new environment variable, a changed directory layout, a new/renamed CI workflow, an updated
Dependabot grouping, a new sibling-repo contract, or a different testing/linting command — update
the relevant section of this file as part of the same change. Treat it the same way you would treat
`README.md`: documentation that goes stale is worse than no documentation, so don't leave it to a
future agent or PR to notice the drift.
