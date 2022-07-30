FROM apache/airflow:2.2.3-python3.8

ENV DEBIAN_FRONTEND noninteractive

USER root
RUN sudo apt-get update && sudo ACCEPT_EULA=Y apt-get upgrade -y -q --assume-yes
RUN sudo apt-get install git gcc -y -q
#RUN sudo apt-get update && sudo apt-get install nodejs
#RUN curl -fsSL https://deb.nodesource.com/setup_lts.x | bash - && apt-get install -y nodejs
RUN apt-get update && apt-get install wget -y
RUN cd /usr/local && mkdir node && cd node && wget https://npm.taobao.org/mirrors/node/v15.8.0/node-v15.8.0-linux-x64.tar.gz && tar -zxvf node-v15.8.0-linux-x64.tar.gz && ln -s /usr/local/node/node-v15.8.0-linux-x64/bin/node /usr/local/bin/node && ln -s /usr/local/node/node-v15.8.0-linux-x64/bin/npm /usr/local/bin/npm && npm install elasticdump -g && ln -s /usr/local/node/node-v15.8.0-linux-x64/lib/node_modules/elasticdump/bin/elasticdump /usr/local/bin/elasticdump
#RUN apt-get install -y nodejs && apt-get install -y npm
USER airflow
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install --no-cache-dir  'grimoire-elk>=0.99.0'
RUN pip install --no-cache-dir git+https://github.com/oss-know/grimoirelab-perceval@development
RUN pip install --no-cache-dir  'pandas>=1.4.0' 'scikit-learn>=1.0.2' 'numpy>=1.21'
RUN pip install --no-cache-dir  'loguru>=0.5.3' \
                                'opensearch-py>=2.0.0' \
                                'PyGithub>=1.54.1' \
                                'GitPython>=3.1.24' \
                                'urllib3>=1.26.7' \
                                'tenacity>=8.0.1' \
                                'requests>=2.26.0' \
                                'clickhouse-driver[lz4,zstd,numpy]>=0.2.2' \
                                'geopy>=2.2.0'\
				'esdk-obs-python'

RUN pip install --no-cache-dir 'apache-airflow[password]==2.2.3'
RUN pip install --no-cache-dir 'multidict==5.2.0'
