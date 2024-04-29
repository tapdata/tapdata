#!/bin/bash

. log.sh

BASE_URI='http://58.251.34.123:29000'
PROJECT_KEY=''
BRANCH=''

which jq > /dev/null
if [[ $? -ne 0 ]]; then
  error "jq is not install."
fi

for arg in "$@"
do
  case $arg in
    --project-key=*)
    PROJECT_KEY="${arg#*=}"
    shift
    ;;
    --branch=*)
    BRANCH="${arg#*=}"
    shift
    ;;
    --sonar-token=*)
    SONAR_TOKEN="${arg#*=}"
    shift
    ;;
  esac
done

if [[ -z $PROJECT_KEY ]]; then
  error 'Project Key is not set.'
fi

if [[ -z $BRANCH ]]; then
  error 'Branch is not set.'
fi

if [[ -z $SONAR_TOKEN ]]; then
  error 'variable $SONAR_TOKEN is not set.'
fi

info "Get Sonar Scan Result"
result=$(curl -L "$BASE_URI/api/qualitygates/project_status?projectKey=$PROJECT_KEY&branch=$BRANCH" -u "$SONAR_TOKEN:" 2>/dev/null)

QUALITY_GATE_STATUS=$(echo $result | jq -r .projectStatus.status)
CONDITIONS=$(echo $result | jq -c .projectStatus.conditions[])

if [[ $QUALITY_GATE_STATUS == "ERROR" ]]; then
  warn "Quality Gate Status: $QUALITY_GATE_STATUS"
  for condition in $CONDITIONS
  do
    status=$(echo "$condition" | jq -r '.status')
    metricKey=$(echo "$condition" | jq -r '.metricKey')
    actualValue=$(echo "$condition" | jq -r '.actualValue')
    if [[ $status == "ERROR" ]]; then
      warn "Status: $status, MetricKey: $metricKey, ActualValue: $actualValue"
    fi
  done
  exit 1
fi
