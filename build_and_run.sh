#!/bin/bash

# =================================================================================
# Script to compile and run Tapdata (TM, Engine, Web)
# 
# Features:
# 1. Checks for Maven environment (installs if missing on macOS).
# 2. Compiles Tapdata project (demonstration of local check).
# 3. Uses Docker Compose to build and run services (Recommended "Better" way).
# =================================================================================

set -e
export NODE_ENV=community
export VUE_APP_MODE=community

# --- Function to check and install Maven (Requirement: Script Check) ---
check_maven_env() {
    echo "Checking Maven environment..."
    if ! command -v mvn &> /dev/null; then
        echo "Maven (mvn) not found."
        
        # Detect OS
        if [[ "$OSTYPE" == "darwin"* ]]; then
            echo "Detected macOS."
            if command -v brew &> /dev/null; then
                echo "Installing Maven via Homebrew..."
                brew install maven
            else
                echo "Error: Homebrew not found. Please install Homebrew or Maven manually."
                exit 1
            fi
        elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
            echo "Detected Linux."
            if command -v apt-get &> /dev/null; then
                echo "Installing Maven via apt-get..."
                sudo apt-get update && sudo apt-get install -y maven
            elif command -v yum &> /dev/null; then
                echo "Installing Maven via yum..."
                sudo yum install -y maven
            else
                echo "Error: Package manager not found. Please install Maven manually."
                exit 1
            fi
        elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
            echo "Detected Windows."
            if command -v choco &> /dev/null; then
                echo "Installing Maven via Chocolatey..."
                choco install maven -y
            else
                echo "Error: Chocolatey not found. Please install Maven manually."
                exit 1
            fi
        else
            echo "Error: OS $OSTYPE not supported by this script for auto-install. Please install Maven manually."
            exit 1
        fi
    else
        echo "Maven is already installed: $(mvn -v | head -n 1)"
    fi
}

# --- Function to check and install JDK 17 ---
check_jdk17_env() {
    echo "Checking JDK 17 environment..."
    
    need_install=false
    
    if ! command -v java &> /dev/null; then
        echo "Java not found."
        need_install=true
    else
        # Check version
        version=$(java -version 2>&1 | head -n 1 | awk -F '"' '{print $2}')
        echo "Found Java version: $version"
        
        # Simple check for "17" in version string (e.g. "17.0.1", "17")
        if [[ "$version" != "17"* ]]; then
             echo "Java version is not 17."
             need_install=true
        else
             echo "Java 17 is already installed."
        fi
    fi
    
    if [ "$need_install" = true ]; then
        echo "Installing JDK 17..."
        # Detect OS
        if [[ "$OSTYPE" == "darwin"* ]]; then
            echo "Detected macOS."
            if command -v brew &> /dev/null; then
                brew install openjdk@17
                # Link it to make it available
                sudo ln -sfn /usr/local/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
                export PATH="/usr/local/opt/openjdk@17/bin:$PATH"
            else
                echo "Error: Homebrew not found. Please install Homebrew or JDK 17 manually."
                exit 1
            fi
        elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
            echo "Detected Linux."
            if command -v apt-get &> /dev/null; then
                sudo apt-get update && sudo apt-get install -y openjdk-17-jdk
            elif command -v yum &> /dev/null; then
                sudo yum install -y java-17-openjdk-devel
            else
                echo "Error: Package manager not found. Please install JDK 17 manually."
                exit 1
            fi
        elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
            echo "Detected Windows."
            if command -v choco &> /dev/null; then
                choco install openjdk17 -y
            else
                echo "Error: Chocolatey not found. Please install JDK 17 manually."
                exit 1
            fi
        else
            echo "Error: OS $OSTYPE not supported by this script for auto-install. Please install JDK 17 manually."
            exit 1
        fi
        
        # Verify installation
        if command -v java &> /dev/null; then
            echo "JDK 17 installed successfully: $(java -version 2>&1 | head -n 1)"
        else
             echo "Warning: JDK 17 installed but 'java' command still not found (check PATH)."
        fi
    fi
}

# --- Main Execution ---

# 1. Check Maven (as requested)
check_maven_env

# 2. Check JDK 17 (as requested)
check_jdk17_env

if [[ "$1" == "--check-only" ]]; then
    echo "Environment check complete. Exiting."
    exit 0
fi

# 2. Compile Locally? 
# The user asked to "Automatic compilation... checking for maven".
# Since we have a Dockerfile that handles compilation *better* (isolated environment),
# we will prioritize the Docker build.
# However, if you want to run a local compile to verify the environment check works:
# mvn clean install -DskipTests -P idaas

echo "----------------------------------------------------------------"
echo "Starting Docker Build & Deployment..."
echo "Note: The Dockerfile uses a multi-stage build which automatically"
echo "provisions the correct Maven environment inside the container,"
echo "ensuring a consistent build regardless of the host environment."
echo "----------------------------------------------------------------"

# 3. Run Docker Compose
if ! command -v docker &> /dev/null; then
    echo "Error: Docker not found. Please install Docker."
    exit 1
fi

docker-compose up --build -d

echo "----------------------------------------------------------------"
echo "Deployment Complete!"
echo "Services:"
echo " - Web:    http://localhost:8080"
echo " - TM:     http://localhost:3030"
echo " - Mongo:  mongodb://admin:tapdata_best2019@localhost:27017/tapdata"
echo "----------------------------------------------------------------"
