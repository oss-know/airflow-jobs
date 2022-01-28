FROM apache/airflow:2.2.3-python3.8

USER root
RUN sudo apt-get update \
  && sudo apt full-upgrade -y \
  && sudo apt-get install git gcc -y

USER airflow
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install --no-cache-dir  'pandas>=1.4.0' 'scikit-learn>=1.0.2' 'numpy>=1.21'
RUN pip install --no-cache-dir  'loguru>=0.5.3' \
                                'opensearch-py>=1.0.0' \
                                'PyGithub>=1.54.1' \
                                'GitPython>=3.1.24' \
                                'urllib3>=1.26.7' \
                                'tenacity>=8.0.1' \
                                'requests>=2.26.0' \
                                'clickhouse-driver[lz4,zstd,numpy]>=0.2.2' \
                                'geopy>=2.2.0'
RUN pip install --no-cache-dir  'grimoire-elk>=0.98.0'

RUN pip install --no-cache-dir 'apache-airflow[password]==2.2.3'
