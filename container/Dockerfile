FROM python:3.13-slim@sha256:2ec5a4a5c3e919570f57675471f081d6299668d909feabd8d4803c6c61af666c

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

CMD run-detection