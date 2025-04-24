FROM python:3.13-slim@sha256:8f3aba466a471c0ab903dbd7cb979abd4bda370b04789d25440cc90372b50e04

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

CMD run-detection