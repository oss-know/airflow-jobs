FROM apache/airflow:2.2.2-python3.8
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install --no-cache-dir numpy pandas scikit-learn opensearch-py
RUN pip install --no-cache-dir GitPython
RUN pip install --no-cache-dir githubpy
<<<<<<< HEAD
#USER root
#RUN apt-get update \
#  && apt-get install git -y
=======
USER root
RUN apt-get update \
  && apt-get install git -y
>>>>>>> 9046e7ebe9626098c5f2b054c11c239267bddd3c
