FROM python:3.12-slim@sha256:740d94a19218c8dd584b92f804b1158f85b0d241e5215ea26ed2dcade2b9d138

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

CMD run-detection