FROM python:3.6-slim-jessie
RUN apt-get update
RUN apt-get install -y gcc
RUN apt-get install -y libmysqlclient-dev
RUN apt-get install -y python-mysqldb
RUN mkdir /cli
COPY main.py requirements.txt /cli/
WORKDIR /cli
RUN pip install --no-cache-dir -r requirements.txt
# will just print help
CMD ["python", "main.py"]