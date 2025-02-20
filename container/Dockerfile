FROM python:3.13-slim@sha256:ae9f9ac89467077ed1efefb6d9042132d28134ba201b2820227d46c9effd3174

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

CMD run-detection