FROM python:3.8

RUN pip install satella>=2.14.24 snakehouse nose2 wheel ujson coverage

ADD tempsdb /app/tempsdb
ADD setup.py /app/setup.py
ADD .coveragerc /app/.coveragerc
ADD setup.cfg /app/setup.cfg
WORKDIR /app

ENV CI=true
RUN python setup.py build_ext --inplace

ADD tests /app/tests

CMD ["coverage", "run", "-m", "nose2", "-vv"]
