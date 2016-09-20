FROM python:3.4

ADD . /usr/src/app
WORKDIR /usr/src/app
RUN pip install -r requirements.txt


COPY ./docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT "/docker-entrypoint.sh"