FROM apache/airflow:2.2.2-python3.8
USER root
RUN apt-get update \
  && apt-get install git -y

RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install --no-cache-dir numpy pandas scikit-learn opensearch-py GitPython githubpy
