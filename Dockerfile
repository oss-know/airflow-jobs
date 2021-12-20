FROM apache/airflow:2.2.2-python3.8
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install --no-cache-dir numpy pandas scikit-learn opensearch-py GitPython githubpy loguru

USER root
RUN sudo apt-get update \
  && sudo apt-get install git -y

USER airflow
