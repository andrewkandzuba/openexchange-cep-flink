#!/usr/bin/env bash

echo "$$_SONAR_TOKEN_SECRET"
mvn clean scoverage:check scoverage:report sonar:sonar -Dsonar.branch.name=$BRANCH_NAME -Dscoverage.minimumCoverage=$_MIN_COVERAGE -B --fail-at-end
