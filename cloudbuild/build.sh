#!/usr/bin/env bash

mvn install -U -P !build-extras -DskipTests=true -Dmaven.javadoc.skip=true -B -V
cp pipeline/target/pipeline-*.jar /jar/
ls /jar/