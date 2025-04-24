FROM python:3.12

WORKDIR /fia_api

COPY . /fia_api

RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && python -m pip install --upgrade pip \
    && python -m pip install --no-cache-dir .

CMD ["uvicorn", "fake_fia_api.fia_api:app", "--host", "0.0.0.0", "--port", "80"]