FROM python:3.13-slim@sha256:6f79e7a10bb7d0b0a50534a70ebc78823f941fba26143ecd7e6c5dca9d7d7e8a

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

COPY ./container/liveness.bash .

CMD run-detection