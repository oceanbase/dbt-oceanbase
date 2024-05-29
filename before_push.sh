#! /bin/bash
# remove unused imports
autoflake --remove-all-unused-imports --in-place --recursive .
# sort imports
isort .
# format the code
black .