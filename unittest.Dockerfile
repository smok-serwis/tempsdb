FROM python:3.8

RUN pip install satella snakehouse nose2

ADD . /app
WORKDIR /app

CMD ["python", "setup.py", "test"]
