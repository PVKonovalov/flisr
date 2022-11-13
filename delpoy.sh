#!/bin/bash

project_name=${PWD##*/}
service_name="${project_name//_/-}"

git pull
go build -o $project_name
systemctl stop $service_name
cp -f $project_name /usr/lib/orxagrid/
systemctl start $service_name
journalctl -u $service_name -f

