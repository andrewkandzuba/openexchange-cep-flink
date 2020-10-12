#!/usr/bin/env bash

mvn clean scoverage:check scoverage:report sonar:sonar -Dsonar.login=$_SONAR_TOKEN_SECRET -Dsonar.branch.name=$BRANCH_NAME -Dscoverage.minimumCoverage=$_MIN_COVERAGE -B --fail-at-end