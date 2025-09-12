FROM python:3.13-slim@sha256:1bca0202e953784ac2a1daf36ebbc9cbebed48afcfba12d1225aaab3793eca33

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

COPY ./container/liveness.bash .

CMD run-detection