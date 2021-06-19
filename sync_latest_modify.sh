#!/bin/bash
git config --global http.sslVerify false
git fetch --all
git reset --hard origin/master
git pull