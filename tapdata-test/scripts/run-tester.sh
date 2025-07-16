#!/bin/bash

# 简化的TapData Connector Performance Tester启动脚本
# 直接使用java命令运行，避免Maven exec插件的模块问题

# 设置颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
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

# 构建classpath
build_classpath() {
    cd "$(dirname "$0")/.." || exit 1
    
    # 获取Maven依赖的classpath
    CLASSPATH=$(mvn dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q 2>/dev/null)
    
    # 添加编译后的classes目录
    CLASSPATH="target/classes:$CLASSPATH"
    
    echo "$CLASSPATH"
}

# 启动测试器
start_tester() {
    print_info "Starting TapData Connector Performance Tester..."
    
    cd "$(dirname "$0")/.." || exit 1
    
    # 构建classpath
    CLASSPATH=$(build_classpath)
    
    if [ -z "$CLASSPATH" ]; then
        print_error "Failed to build classpath"
        exit 1
    fi
    
    # Java 9+ 兼容性参数
    JVM_ARGS=""
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
    
    # 调试设置（可选）
    if [ "$DEBUG" = "true" ]; then
        JVM_ARGS="$JVM_ARGS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
        print_info "Debug mode enabled on port 5005"
    fi
    
    print_info "JVM Args: $JVM_ARGS"
    print_info "Classpath: ${CLASSPATH:0:100}..."
    
    # 启动Java程序
    java $JVM_ARGS -cp "$CLASSPATH" io.tapdata.test.connector.ConnectorTesterMain
}

# 显示帮助信息
show_help() {
    echo "TapData Connector Performance Tester (Direct Java Runner)"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help     Show this help message"
    echo "  -d, --debug    Enable debug mode"
    echo "  -c, --compile  Force recompile before starting"
    echo "  --skip-compile Skip compilation step"
    echo ""
    echo "Environment Variables:"
    echo "  DEBUG=true     Enable debug mode"
    echo "  JAVA_HOME      Java installation directory"
    echo ""
    echo "Examples:"
    echo "  $0                    # Start with default settings"
    echo "  $0 --debug            # Start with debug mode"
    echo "  $0 --compile          # Force recompile and start"
    echo "  DEBUG=true $0         # Start with debug mode via env var"
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
    
    print_info "=== TapData Connector Performance Tester (Direct Java) ==="
    
    # 检查环境
    check_java
    
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
