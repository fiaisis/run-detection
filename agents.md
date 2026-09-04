# AI Agents Documentation

This document provides guidance for AI agents (like Claude, Junie, etc.) working on the `run-detection` repository.

## Project Overview

The `run-detection` service sits in the ISIS data reduction pipeline. It consumes RabbitMQ messages announcing new instrument data files (the ingress queue), ingests metadata from the referenced NeXus/HDF5 files, verifies each run against per-instrument specification rules, and publishes reduction job requests downstream (the egress queue). Runs that fail verification are routed to the failure queue.

### Core Technologies
- **Python**: 3.12+
- **pika**: RabbitMQ client (quorum queues, producer/consumer).
- **h5py**: NeXus/HDF5 file access.
- **xmltodict**: XML parsing (e.g. instrument journal files).
- **requests**: Client for the FIA API (remote instrument specifications).
- **Pytest**: Testing framework (with `pytest-random-order` and `pytest-cov`).
- **Ruff**: Linting and formatting.
- **Mypy**: Type checking (run with `--strict` in CI).
- **Docker**: Containerization.

## Repository Structure

- `rundetection/`: Main package.
    - `run_detection.py`: Application entry point (`run-detection` CLI), RabbitMQ producer/consumer setup, heartbeat.
    - `job_requests.py`: `JobRequest` model.
    - `specifications.py`: `InstrumentSpecification`, including fetching specs from the FIA API.
    - `health.py`: Heartbeat.
    - `exceptions.py`: Service exceptions.
    - `ingestion/`: File metadata ingestion.
        - `ingest.py`: NeXus ingestion logic.
        - `extracts.py`: Instrument-specific metadata extraction functions and the extraction factory.
    - `rules/`: Run verification rules.
        - `rule.py`: Base `Rule` class.
        - `factory.py`: `RuleFactory` mapping specification keys to rule implementations.
        - `common_rules.py` plus one module per instrument (e.g. `enginx_rules.py`, `imat_rules.py`, `mari_rules.py`, `osiris_rules.py`, `vesuvio_rules.py`, ...).
- `test/`: Test suite.
    - `test_e2e.py`: End-to-end test (requires the docker compose stack).
    - `rules/`, `ingestion/`: Unit tests, mirroring the package layout.
    - `docker-compose.yml`: E2E stack (`rabbit-mq`, `fake-fia-api`, `run-detection`).
    - `e2e_components/`: Fake FIA API used by e2e tests.
    - `test_data/`: NeXus files and per-instrument specification JSON files.
- `container/`: `Dockerfile` and `liveness.bash` for the service container.
- `tools/`: Utility scripts (`specification_generator.py`, `specification_migration.py`).
- `pyproject.toml`: Project metadata and dependencies.

## Development Workflow

### Setup

The project aims to be agnostic regarding local developer setups.

#### Using Conda (Recommended if available)
If you use conda, check if the `run-detection` environment exists. If not, create it and install dependencies:

```bash
# Check if conda is installed
which conda

# Check if environment exists
conda env list | grep run-detection

# If it doesn't exist, create it (Python 3.12+)
conda create -n run-detection "python>=3.12" -y

# Activate and install dependencies
conda activate run-detection
pip install .[all]
```

#### Using Pip/Venv (Fallback if conda is not available)
If conda is not installed or you prefer standard virtual environments:

```bash
python -m venv venv
source venv/bin/activate
pip install .[all]
```

### Running the Service (Development)
```bash
pip install .
run-detection
```

The service is configured via environment variables (see [Configuration](#configuration)). With no configuration it connects to a RabbitMQ broker on `localhost` as `guest`/`guest` and uses the default queue names.

### Running Tests

Unit tests:
```bash
pytest . --ignore test/test_e2e.py
```

E2E tests (start the docker compose stack in `test/` first, which builds the run-detection container from `container/Dockerfile`):
```bash
cd test
docker compose up -d
cd ..
pytest test/test_e2e.py
```

Any code changes made after starting run-detection require the run-detection container to be rebuilt (`docker compose up -d --build`).

### Branch Naming

Branches should use snake_case descriptions of the work being done. Do not use category prefixes (like `feature/` or `fix/`).

- **Format**: `description_of_work` (e.g., `update_agent_guidelines`).

### Pull Request Process

1. **Local Validation**: Before opening a PR, ensure all tests pass and code quality checks are green. Use the conda environment:
   ```bash
   conda run -n run-detection pytest . --ignore test/test_e2e.py
   conda run -n run-detection ruff check .
   conda run -n run-detection ruff format .
   conda run -n run-detection mypy --strict rundetection
   ```
   **Note**: The CI formatting workflow (`formatting_and_linting.yml`) also runs `ruff format .`, `ruff check --fix`, and `mypy --strict rundetection`, and automatically commits and pushes any fixes to your branch as a "Formatting and linting commit".
2. **PR Title**: Pull request titles should use a clear, concise description in normal grammar (e.g., `Update agent guidelines`). Do not use snake_case for PR titles.
3. **PR Description**:
   - Linking the issue: `Closes # <Issue Number>` (If an issue number is not evident from the task, ask the user for it or confirm if there is no issue number to link).
   - A "## Description" header followed by an explanation of changes.

### Handling Dependabot PRs

Dependabot periodically creates pull requests (grouped as `python-packages`, `docker/container`, and `github_actions/action-packages`) to update dependencies and GitHub Actions. Agents should follow this workflow:

1. **List and Identify**: Use `gh pr list` to find open Dependabot PRs.
2. **Review Changes**: Inspect the changes with `gh pr diff <number>`. Pay close attention to major version bumps in `pyproject.toml`, `container/Dockerfile`, or changes in `.github/workflows/`.
3. **Validation**:
   - **Python/Dependency changes**: If `pyproject.toml` or Python files are modified, perform local validation:
     - Checkout the PR branch: `gh pr checkout <number>`.
     - Run the test suite: `pytest . --ignore test/test_e2e.py`.
     - Run linting, formatting, and type checking: `ruff check .`, `ruff format .`, and `mypy --strict rundetection`.
   - **Docker/Action changes**: If `container/Dockerfile` or `.github/workflows/` are modified, check that the actions have passed as part of the PR:
     - `gh pr checks <number>`
4. **Approve and Merge**:
   - If validation passes, approve the PR: `gh pr review <number> --approve`.
   - Merge the PR: `gh pr merge <number> --merge`.
   - **Note**: Use the `--admin` flag if the merge is blocked by branch protection policies (e.g., `gh pr merge <number> --merge --admin`).
5. **Clean up**: Close redundant or superseded PRs using `gh pr close <number> --delete-branch`.
6. **Final Verification**: After merging, return to the `main` branch, pull the latest changes, and run `pytest . --ignore test/test_e2e.py` one last time to ensure everything is correct.

### Agent Guidelines

#### Environment Setup
Agents should prefer using a conda environment named `run-detection` if conda is available on the system PATH.

If conda is not available, the agent should fallback to using a standard Python virtual environment (`venv`).

If an environment does not exist, the agent should offer to create it and install requirements from `pyproject.toml`.

### Configuration
The service is driven by environment variables:
- `QUEUE_HOST`: RabbitMQ host (default `localhost`).
- `QUEUE_USER`: RabbitMQ username (default `guest`).
- `QUEUE_PASSWORD`: RabbitMQ password (default `guest`).
- `INGRESS_QUEUE_NAME`: Queue to consume file events from (default `watched-files`).
- `EGRESS_QUEUE_NAME`: Queue to publish job requests to (default `scheduled-jobs`).
- `FAILURE_QUEUE_NAME`: Queue to publish failed runs to (default `failed-watched-files`).
- `FIA_API_URL`: Base URL of the FIA API for remote instrument specifications (default `http://localhost:8000`).
- `FIA_API_API_KEY`: API key required to fetch specifications from the FIA API.
- `IMAT_DIR`: Base directory for IMAT data, used by `imat_rules.py` (default `/imat`).

When working with file paths, use `pathlib.Path` and be aware that the service expects instrument data to be laid out under a mounted archive directory (e.g. `/archive` in the container, see `test/docker-compose.yml`).

### Instrument Rules and Specifications
For a run to be sent downstream, the metadata of the received file must meet the specification for that instrument.

- **Specifications**: Per-instrument JSON files (`<instrument>_specification.json`) live in `test/test_data/specifications/` for tests and are fetched from the FIA API in production (see `specifications.py`). Each field is a `Rule`.
- **Adding a rule**: Implement a `Rule` subclass in the appropriate `rules/` module, register it in the `RuleFactory` in `rules/factory.py`, and add the field to the relevant specification file. See `README.md` for a worked example.
- **Adding extraction functions**: Instrument-specific metadata extraction lives in `ingestion/extracts.py`; add an extraction function and register it in the extraction factory. See `README.md` for the expected signature.

### Testing
- Always include/update tests, mirroring the existing layout (e.g. `test/rules/test_<instrument>_rules.py`).
- Test NeXus data and specification files live in `test/test_data/`; add new data there when testing new instruments or rules.
- E2E tests (`test/test_e2e.py`) require the docker compose stack in `test/` (RabbitMQ + fake FIA API) and are excluded from the default unit test run.

### Formatting
The project uses `Ruff` for linting and formatting (line length 120, rules in `pyproject.toml`) and `Mypy` with `--strict` on the `rundetection` package. Ensure your changes comply with both before opening a PR.
