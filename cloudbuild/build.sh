#!/usr/bin/env bash

mvn install -U -DskipTests=true -Dmaven.javadoc.skip=true -B -V
cp pipeline/target/pipeline-*.jar /jar/
ls /jar/