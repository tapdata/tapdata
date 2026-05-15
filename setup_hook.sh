# This script defines a shell function wrapper for docker-compose
# Source this file in your .zshrc or .bashrc to enable auto-checking.
# Usage: source setup_hook.sh

docker-compose() {
    # Check if the command is "up"
    # We check if arguments contain "up"
    local is_up=false
    for arg in "$@"; do
        if [[ "$arg" == "up" ]]; then
            is_up=true
            break
        fi
    done

    if [ "$is_up" = true ]; then
        echo "========================================================"
        echo " [Auto-Check] Running Pre-flight Environment Checks..."
        echo "========================================================"
        
        # Determine the path to build_and_run.sh relative to where we are,
        # or assume it's in the current directory if we are running from project root.
        if [ -f "./build_and_run.sh" ]; then
             ./build_and_run.sh --check-only
             if [ $? -ne 0 ]; then
                 echo "Error: Environment check failed."
                 return 1
             fi
        else
            echo "Warning: build_and_run.sh not found in current directory. Skipping checks"
        fi
        echo "========================================================"
        echo " [Auto-Check] Checks Passed. Starting Docker Compose..."
        echo "========================================================"
    fi

    # Execute the original docker-compose command
    command docker-compose "$@"
}

echo "docker-compose hook loaded. 'docker-compose up' will now automatically run environment checks"