FROM python:3.13-slim@sha256:23a81be7b258c8f516f7a60e80943cace4350deb8204cf107c7993e343610d47

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

CMD run-detection