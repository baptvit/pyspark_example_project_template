#!/usr/bin/env bash

pip3 install pipenv
#pipenv install
#pipenv install
pipenv install --dev    
#pipenv shell
pipenv run python -m unittest tests/test_*.py
