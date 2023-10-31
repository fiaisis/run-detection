FROM python:3.12-slim

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

CMD run-detection