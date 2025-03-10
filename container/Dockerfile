FROM python:3.13-slim@sha256:f3614d98f38b0525d670f287b0474385952e28eb43016655dd003d0e28cf8652

# Install run-detection to the container
ADD . .
RUN python -m pip install --no-cache-dir .

CMD run-detection