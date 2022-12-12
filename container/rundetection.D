FROM python:3.10

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

CMD run-detection