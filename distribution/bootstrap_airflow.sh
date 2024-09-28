#!/bin/bash

echo "airflow metadb is not ready, begin to init it..."
airflow db init
echo "airflow metadb is ready."

exec "$@"