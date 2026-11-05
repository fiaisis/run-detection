FROM python:3.15-slim@sha256:8c13a3f37a6a5b71498228e902c4b9f51c1f7900d49fabf71f9a650c6120e0d0

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

COPY ./container/liveness.bash .

CMD run-detection