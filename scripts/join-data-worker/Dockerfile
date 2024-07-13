FROM python:3.12.4-slim

RUN apt-get update \
  && apt-get -y install libpq-dev gcc \
  && pip install psycopg2


WORKDIR /usr/src/app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "./main.py"]

