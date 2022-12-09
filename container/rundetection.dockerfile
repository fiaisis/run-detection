FROM ubuntu:20.04

# Install run-detection to the container
ADD . .
RUN python3 -m pip install --user --no-cache-dir .

CMD run-detection