#!/bin/bash

. log.sh

BASE_URI='http://fileserver.tapdata.io:29000'
PROJECT_KEY=''
BRANCH=''
GITHUB_TOKEN=""
OWNER="tapdata"
REPO=""
PR_NUMBER=
SONAR_PULL_REQUEST=

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
    --github-token=*)
    GITHUB_TOKEN="${arg#*=}"
    shift
    ;;
    --repo=*)
    REPO="${arg#*=}"
    shift
    ;;
    --pr-number=*)
    PR_NUMBER="${arg#*=}"
    shift
    ;;
    --pull-request=*)
    SONAR_PULL_REQUEST="${arg#*=}"
    shift
    ;;
  esac
done

if [[ -z $PROJECT_KEY ]]; then
  error 'Project Key is not set.'
fi

if [[ -z $BRANCH && -z $SONAR_PULL_REQUEST ]]; then
  error 'Branch is not set.'
fi

if [[ -z $SONAR_TOKEN ]]; then
  error 'variable $SONAR_TOKEN is not set.'
fi

info "Get Sonar Scan Result"
query=(--data-urlencode "projectKey=$PROJECT_KEY")
if [[ -n "$SONAR_PULL_REQUEST" ]]; then
  [[ "$SONAR_PULL_REQUEST" =~ ^[1-9][0-9]*$ ]] || error "Invalid Sonar pull request key"
  query+=(--data-urlencode "pullRequest=$SONAR_PULL_REQUEST")
  result_link="$BASE_URI/dashboard?id=$PROJECT_KEY&pullRequest=$SONAR_PULL_REQUEST"
else
  query+=(--data-urlencode "branch=$BRANCH")
  encoded_branch=$(printf '%s' "$BRANCH" | jq -sRr @uri)
  result_link="$BASE_URI/dashboard?id=$PROJECT_KEY&branch=$encoded_branch"
fi
result=$(curl -L --get "$BASE_URI/api/qualitygates/project_status" "${query[@]}" -u "$SONAR_TOKEN:" 2>/dev/null)

QUALITY_GATE_STATUS=$(echo $result | jq -r .projectStatus.status)
CONDITIONS=$(echo $result | jq -c .projectStatus.conditions[])

COMMENT="SonarQube Quality Gate Status: **$QUALITY_GATE_STATUS**\n\n"

if [[ $QUALITY_GATE_STATUS == "ERROR" ]]; then
  warn "Quality Gate Status: $QUALITY_GATE_STATUS"
  for condition in $CONDITIONS
  do
    status=$(echo "$condition" | jq -r '.status')
    metricKey=$(echo "$condition" | jq -r '.metricKey')
    actualValue=$(echo "$condition" | jq -r '.actualValue')
    if [[ $status == "ERROR" ]]; then
      warn "Status: $status, MetricKey: $metricKey, ActualValue: $actualValue"
      COMMENT+="- Status: **$status**, MetricKey: **$metricKey**, ActualValue: **$actualValue**\n"
    fi
  done
  COMMENT+="\n\nSee Sonar Scan Result at: $result_link"
  info "Send message to Github Pr Comment"
  if [[ -z $PR_NUMBER ]]; then
    warn "variable PR_NUMBER is not set, sending termination."
  else
    curl -s -H "Authorization: token $GITHUB_TOKEN" -X POST -d "{\"body\": \"$COMMENT\"}" "https://api.github.com/repos/$OWNER/$REPO/issues/$PR_NUMBER/comments" > /dev/null
    if [[ $? -ne 0 ]]; then
      error "Send message to Github Pr Comment Failed"
    fi
  fi
fi
