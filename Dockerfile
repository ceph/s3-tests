FROM python:3.6.15-slim-bullseye as s3-tests
WORKDIR /s3-tests
COPY . /s3-tests
RUN pip3 install -r requirements.txt
