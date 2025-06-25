#!/bin/bash

# TapData Connector Performance Tester 启动脚本

# 设置颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查Java环境
check_java() {
    if ! command -v java &> /dev/null; then
        print_error "Java is not installed or not in PATH"
        exit 1
    fi
    
    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2)
    print_info "Java version: $JAVA_VERSION"
}

# 检查Maven环境
check_maven() {
    if ! command -v mvn &> /dev/null; then
        print_error "Maven is not installed or not in PATH"
        exit 1
    fi
    
    MVN_VERSION=$(mvn -version | head -n 1)
    print_info "Maven version: $MVN_VERSION"
}

# 编译项目
compile_project() {
    print_info "Compiling project..."
    
    cd "$(dirname "$0")/.." || exit 1
    
    if mvn clean compile -q; then
        print_success "Project compiled successfully"
    else
        print_error "Failed to compile project"
        exit 1
    fi
}

# 设置JVM参数
setup_jvm_args() {
    JVM_ARGS=""

    # Java 9+ 模块系统兼容性参数
    JVM_ARGS="$JVM_ARGS --add-opens=java.base/java.lang=ALL-UNNAMED"
    JVM_ARGS="$JVM_ARGS --add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
    JVM_ARGS="$JVM_ARGS --add-opens=java.base/java.net=ALL-UNNAMED"
    JVM_ARGS="$JVM_ARGS --add-opens=java.base/java.security=ALL-UNNAMED"
    JVM_ARGS="$JVM_ARGS --add-opens=java.base/java.util=ALL-UNNAMED"
    JVM_ARGS="$JVM_ARGS --add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
    JVM_ARGS="$JVM_ARGS --add-opens=java.base/sun.net.util=ALL-UNNAMED"
    JVM_ARGS="$JVM_ARGS --add-opens=java.base/sun.security.util=ALL-UNNAMED"

    # 内存设置
    JVM_ARGS="$JVM_ARGS -Xms512m -Xmx2g"

    # GC设置
    JVM_ARGS="$JVM_ARGS -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

    # 日志设置
    if [ "$VERBOSE" = "true" ]; then
        JVM_ARGS="$JVM_ARGS -Dlogback.configurationFile=logback.xml"
        print_info "Verbose logging enabled"
    else
        JVM_ARGS="$JVM_ARGS -Dlogback.configurationFile=logback-simple.xml"
        print_info "Simple logging enabled (use --verbose for detailed logs)"
    fi

    # 调试设置（可选）
    if [ "$DEBUG" = "true" ]; then
        JVM_ARGS="$JVM_ARGS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
        print_info "Debug mode enabled on port 5005"
    fi

    export MAVEN_OPTS="$JVM_ARGS"
}

# 启动测试器
start_tester() {
    print_info "Starting TapData Connector Performance Tester..."
    print_info "JVM Args: $MAVEN_OPTS"
    
    mvn exec:java -Dexec.mainClass="io.tapdata.test.connector.ConnectorTesterMain" -q
}

# 显示帮助信息
show_help() {
    echo "TapData Connector Performance Tester"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help     Show this help message"
    echo "  -d, --debug    Enable debug mode"
    echo "  -v, --verbose  Enable verbose logging"
    echo "  -c, --compile  Force recompile before starting"
    echo "  --skip-compile Skip compilation step"
    echo ""
    echo "Environment Variables:"
    echo "  DEBUG=true     Enable debug mode"
    echo "  VERBOSE=true   Enable verbose logging"
    echo "  JAVA_HOME      Java installation directory"
    echo "  MAVEN_HOME     Maven installation directory"
    echo ""
    echo "Examples:"
    echo "  $0                    # Start with default settings (simple logging)"
    echo "  $0 --verbose          # Start with verbose logging"
    echo "  $0 --debug            # Start with debug mode"
    echo "  $0 --compile          # Force recompile and start"
    echo "  VERBOSE=true $0       # Start with verbose logging via env var"
}

# 主函数
main() {
    local FORCE_COMPILE=false
    local SKIP_COMPILE=false
    
    # 解析命令行参数
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -d|--debug)
                DEBUG=true
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -c|--compile)
                FORCE_COMPILE=true
                shift
                ;;
            --skip-compile)
                SKIP_COMPILE=true
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    print_info "=== TapData Connector Performance Tester ==="
    
    # 检查环境
    check_java
    check_maven
    
    # 设置JVM参数
    setup_jvm_args
    
    # 编译项目
    if [ "$SKIP_COMPILE" != "true" ]; then
        if [ "$FORCE_COMPILE" = "true" ] || [ ! -d "target/classes" ]; then
            compile_project
        else
            print_info "Skipping compilation (target/classes exists)"
        fi
    else
        print_info "Skipping compilation as requested"
    fi
    
    # 启动测试器
    start_tester
}

# 捕获中断信号
trap 'print_info "Shutting down..."; exit 0' INT TERM

# 运行主函数
main "$@"
