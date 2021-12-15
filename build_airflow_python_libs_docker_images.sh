#!/usr/bin/env bash
docker build . -f Dockerfile --tag airflowjobs/airflow:dev-python3.8
#docker build . -f Dockerfile --tag airflowjobs/airflow:0.0.5-python3.8