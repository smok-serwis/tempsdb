FROM python:3.8

RUN pip install satella snakehouse nose2 wheel ujson

ADD tempsdb /app/tempsdb
ADD setup.py /app/setup.py
ADD setup.cfg /app/setup.cfg
WORKDIR /app
RUN python setup.py build_ext --inplace

ADD tests /app/tests

CMD ["nose2", "-vv"]
