#!/bin/bash
# Wrapper script to automatically run environment checks before any command (defaulting to docker-compose up -d)

# Get the directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Interactive Mode Selection
echo "Please select the running mode:"
echo "1) Local Build (Build from source code)"
echo "2) Remote Image (Use official image ghcr.io/tapdata/tapdata:latest)"
read -p "Enter your choice [1]: " mode
mode=${mode:-1}

COMPOSE_FILES="-f docker-compose.yml"

if [ "$mode" == "1" ]; then
    echo "Selected Mode: Local Build"
    COMPOSE_FILES="$COMPOSE_FILES -f docker-compose.local.yml"
    
    # Run environment checks using the existing build_and_run.sh with --check-only flag
    echo "Running auto-environment checks..."
    "$DIR/build_and_run.sh" --check-only

    # Check exit code of the check script
    if [ $? -ne 0 ]; then
        echo "Environment checks failed. Aborting."
        exit 1
    fi
    echo "Environment checks passed."

elif [ "$mode" == "2" ]; then
    echo "Selected Mode: Remote Image"
    COMPOSE_FILES="$COMPOSE_FILES -f docker-compose.remote.yml"
    echo "Skipping local environment checks for remote image mode."
else
    echo "Invalid choice. Exiting."
    exit 1
fi

echo "Proceeding with command..."

# Run the requested command. If no arguments provided, default to 'docker-compose up -d'
if [ $# -eq 0 ]; then
    # We need to construct the full command with the correct compose files
    echo "Executing: docker-compose $COMPOSE_FILES up -d --build"
    docker-compose $COMPOSE_FILES up -d --build
else
    # Run the user-provided command, injecting the compose files
    # Note: This is a bit tricky if the user provides their own flags. 
    # For simplicity, we prepend the compose files.
    echo "Executing: docker-compose $COMPOSE_FILES $@"
    docker-compose $COMPOSE_FILES "$@"
fi
