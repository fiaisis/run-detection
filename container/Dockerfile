FROM python:3.13-slim@sha256:026dd417a88d0be8ed5542a05cff5979d17625151be8a1e25a994f85c87962a5

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

CMD run-detection