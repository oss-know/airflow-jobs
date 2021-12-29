FROM apache/airflow:2.2.3-python3.8
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install --no-cache-dir  numpy pandas scikit-learn
RUN pip install --no-cache-dir  loguru==0.5.3 \
                                opensearch-py==1.0.0 \
                                PyGithub==1.54.1 \
                                GitPython==3.1.24 \
                                urllib3==1.26.7 \
                                tenacity==8.0.1 \
                                requests==2.26.0
RUN pip install 'apache-airflow[password]'
USER root
RUN sudo apt-get update \
  && sudo apt-get install git -y

USER airflow
