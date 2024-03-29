FROM apache/airflow:2.5.2-python3.8

ENV DEBIAN_FRONTEND noninteractive

USER root
RUN sudo apt-get update && sudo ACCEPT_EULA=Y apt-get upgrade -y -q --assume-yes
RUN sudo apt-get install git gcc -y -q
RUN sudo apt-get install libmysqlclient-dev -y

USER airflow
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install --no-cache-dir  'grimoire-elk>=0.99.0'
RUN pip install --no-cache-dir git+https://github.com/oss-know/grimoirelab-perceval@development
RUN pip install --no-cache-dir  'pandas>=1.4.0' 'scikit-learn>=1.0.2' 'numpy>=1.21' 'pydriller==2.5'
RUN pip install --no-cache-dir  'loguru>=0.5.3' \
                                'opensearch-py>=2.0.0' \
                                'PyGithub>=1.54.1' \
                                'GitPython>=3.1.24' \
                                'urllib3>=1.26.7' \
                                'tenacity>=8.0.1' \
                                'requests>=2.26.0' \
                                'clickhouse-driver[lz4,zstd,numpy]>=0.2.2' \
                                'zulip~=0.8.2' \
                                'geopy>=2.2.0'

RUN pip install --no-cache-dir 'apache-airflow[password]==2.5.2'
RUN pip install --no-cache-dir 'multidict==5.2.0'
RUN pip install --no-cache-dir 'networkx~=3.1'

RUN pip install 'mysql-connector-python~=8.1.0'
RUN pip install 'lizard~=1.17.10'
RUN pip install 'timeout-decorator~=0.5.0'

RUN pip install markupsafe==2.0.1
