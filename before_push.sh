#! /bin/bash
# remove unused imports
autoflake --remove-all-unused-imports --in-place --recursive .
# sort the imports
isort --check .
# format the code
black .