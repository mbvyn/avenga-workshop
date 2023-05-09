FROM apache/airflow

USER root

RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

RUN airflow db init
RUN airflow users create -u airflow -p airflow -f Admin -l Admin -r Admin -e admin@admin.com

RUN pip install --upgrade pip \
    && pip install pyspark==3.2.2 \
    && pip install apache-airflow-providers-apache-spark

COPY ./airflow_start.sh /usr/bin/airflow_start.sh
ENTRYPOINT [ "airflow_start.sh" ]