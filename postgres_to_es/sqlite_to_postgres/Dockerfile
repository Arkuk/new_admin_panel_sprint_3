FROM python:3.10

WORKDIR /sqlite_to_postgres

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt requirements.txt
RUN apt update -y  \
    && apt install netcat -y \
    && pip install --upgrade pip  \
    && pip install -r requirements.txt

COPY . .

ENTRYPOINT ["sh","docker-entrypoint.sh"]