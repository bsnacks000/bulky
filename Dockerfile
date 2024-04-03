FROM bsnacks000/python-poetry:3.11-1.3.2 as base 

RUN curl -sSf https://atlasgo.sh | sh

RUN mkdir app 
WORKDIR /app  
ADD . /app
COPY . /app 

ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.9.0/wait /wait
RUN chmod +x /wait \
    && pip install -U pip \
    && pip install -r requirements.txt \
    && apt update -y \
    && apt install -y postgresql-client

EXPOSE 8080