FROM apache/airflow:2.2.2-python3.8
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install --no-cache-dir numpy pandas scikit-learn opensearch-py
RUN pip install --no-cache-dir GitPython
RUN pip install --no-cache-dir githubpy
USER root
RUN apt-get update \
  && apt-get install git -y
