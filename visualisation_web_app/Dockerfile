FROM python:3.7-slim

RUN adduser --disabled-password --gecos "" ofb

WORKDIR /home/app

COPY requirements.txt ./requirements.txt
RUN python -m venv venv
RUN venv/bin/pip install -r requirements.txt
RUN venv/bin/pip install gunicorn

COPY app.py ./app.py
COPY wsgi.py ./wsgi.py
COPY data_cloud ./data_cloud
COPY assets ./assets
COPY boot.sh ./boot.sh
COPY config.cfg ./config.cfg
RUN chmod +x ./boot.sh

RUN chown -R ofb:ofb ./
USER ofb

EXPOSE 5000
ENTRYPOINT ["./boot.sh"]