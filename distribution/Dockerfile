# The Dockerfile is built for OB Cloud AP
FROM apache/airflow:2.9.3-python3.9

LABEL maintainer="ODC <odc@oceanbase.com>"
LABEL description="OB Cloud compute module"

ARG AIRFLOW_INSTALL_DIR=/opt/airflow
ARG DBT_CORE_VERSION=1.8.3
ARG DBT_OCEANBASE_ADAPTER_BRANCH=dev/1.0.x

ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.utf-8

RUN pip install dbt-core==${DBT_CORE_VERSION}

RUN git clone https://github.com/oceanbase/dbt-oceanbase.git --branch ${DBT_OCEANBASE_ADAPTER_BRANCH} \
	&& cd dbt-oceanbase \
	&& pip install . \
	&& cd ../ \
	&& rm -rf dbt-oceanbase

RUN mkdir -p ${AIRFLOW_INSTALL_DIR}
WORKDIR ${AIRFLOW_INSTALL_DIR}