FROM python:3.13-slim@sha256:58c30f5bfaa718b5803a53393190b9c68bd517c44c6c94c1b6c8c172bcfad040

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

COPY ./container/liveness.bash .

CMD run-detection