FROM python:3.14-slim@sha256:9b9a75b908891c42b7af174dcf3f6534ebcedfc28c874c6281eb452e86470e3e

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

COPY ./container/liveness.bash .

CMD run-detection