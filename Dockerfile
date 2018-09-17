FROM python:3.5

ADD ./ /code/

#Fix for SSL error, see https://github.com/neo4j/neo4j/issues/9233
RUN echo "deb http://httpredir.debian.org/debian/ jessie-backports main" >> /etc/apt/sources.list && \
  apt update && apt install -y -t jessie-backports openssl

RUN  apt-get install netcat -y


RUN cd /code/ && python setup.py install

WORKDIR /code

RUN rm -r -f  ./build
RUN rm -r -f  ./.eggs
RUN rm -r -f  ./env
RUN rm -r -f  ./env2
RUN rm -r -f  ./dist

CMD ["bash", "./start.sh", "python", "setup.py", "test"]