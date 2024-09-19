FROM python:3.12-slim@sha256:15bad989b293be1dd5eb26a87ecacadaee1559f98e29f02bf6d00c8d86129f39

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

CMD run-detection