FROM godatadriven/pyspark:3.0.2-buster

ENV PYSPARK_MAJOR_PYTHON_VERSION=3

WORKDIR /

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY . .
