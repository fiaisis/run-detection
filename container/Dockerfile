FROM python:3.13-slim@sha256:751d8bece269ba9e672b3f2226050e7e6fb3f3da3408b5dcb5d415a054fcb061

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

CMD run-detection