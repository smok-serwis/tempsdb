FROM python:3.8

RUN pip install satella snakehouse nose2 wheel

ADD . /app
WORKDIR /app
RUN python setup.py build_ext --inplace

CMD ["nose2", "-vv"]
