FROM python:3.14-slim@sha256:e3782138420d9e87c16497320817db6ca8689e354d6150426dd6fc261f3fd682

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

COPY ./container/liveness.bash .

CMD run-detection