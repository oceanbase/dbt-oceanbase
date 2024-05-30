#! /bin/bash
# remove unused imports
autoflake --remove-all-unused-imports --in-place --recursive .
# format the code
black .