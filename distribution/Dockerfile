# The Dockerfile is built for OB Cloud AP
FROM apache/airflow:2.9.3-python3.9

LABEL maintainer="ODC <odc@oceanbase.com>"
LABEL description="OB Cloud compute module"

ARG DBT_CORE_VERSION=1.8.3
ARG DBT_OCEANBASE_VERSION=0.0.1

RUN pip install dbt-core==${DBT_CORE_VERSION}

RUN mkdir -p dbt-oceanbase

COPY ./ ./dbt-oceanbase

RUN cd dbt-oceanbase \
    && pip install . \
	&& cd ../

USER root

RUN rm -rf dbt-oceanbase

COPY distribution/bootstrap_airflow.sh /usr/local/bin/bootstrap_airflow.sh

RUN chmod +x /usr/local/bin/bootstrap_airflow.sh

USER airflow

ENTRYPOINT ["/usr/local/bin/bootstrap_airflow.sh"]