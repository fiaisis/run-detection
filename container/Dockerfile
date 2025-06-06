FROM python:3.13-slim@sha256:d97b595c5f4ac718102e5a5a91adaf04b22e852961a698411637c718d45867c8

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

COPY ./container/liveness.bash .

CMD run-detection