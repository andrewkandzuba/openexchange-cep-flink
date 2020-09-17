#!/usr/bin/env bash

gpg --fast-import /root/.gpg/gpg.key
cp /jar/pipeline-*.jar pipeline/target/
mvn deploy -P sign,build-extras,docker -DskipTests=true --settings cd/mvnsettings.xml