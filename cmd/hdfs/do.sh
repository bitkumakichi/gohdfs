#!/usr/bin/bash
CGO_ENABLED=0 go build  -o hdfs .
cp hdfs ~/bin/hdfs
