FROM python:3.13-slim@sha256:d8130429ac8488cd869c7e7710e733aedc87bdd0d4bf816af831925c74b86e3f

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

COPY ./container/liveness.bash .

CMD run-detection