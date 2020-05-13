FROM python:3.7-slim-stretch
ADD . /slave
WORKDIR /slave
RUN pip install -r requirements.txt
CMD python user.py
