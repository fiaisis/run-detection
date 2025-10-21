FROM python:3.14-slim@sha256:94ce8aa707ac74c5469ebca60581cca0743af23f4dd2eb885bee3c13db563b7c

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

COPY ./container/liveness.bash .

CMD run-detection