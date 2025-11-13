FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev build-essential cmake libssl-dev libffi-dev libpq-dev && \
    apt-get clean

WORKDIR /app

COPY requirements.txt /app/

RUN python3 -m pip install --upgrade pip setuptools wheel && \
    pip3 install --no-cache-dir -r requirements.txt

COPY . /app