#!/bin/bash

# TapData Connector Tester - 构建可执行JAR脚本

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
    JAVA_MAJOR_VERSION=$(echo $JAVA_VERSION | cut -d'.' -f1)
    
    print_info "Java version: $JAVA_VERSION"
    
    if [ "$JAVA_MAJOR_VERSION" -lt "17" ]; then
        print_error "Java 17 or higher is required. Current version: $JAVA_VERSION"
        exit 1
    fi
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

# 选择POM文件
select_pom() {
    local pom_file="pom.xml"
    
    if [ "$USE_SHADE" = "true" ]; then
        pom_file="pom-shade.xml"
        print_info "Using Shade plugin POM: $pom_file"
    elif [ "$USE_JDK17" = "true" ]; then
        pom_file="pom-jdk17.xml"
        print_info "Using JDK 17 POM: $pom_file"
    else
        print_info "Using default POM: $pom_file"
    fi
    
    echo "$pom_file"
}

# 构建项目
build_project() {
    local pom_file=$1
    
    print_info "Building project with $pom_file..."
    
    cd "$(dirname "$0")/.." || exit 1
    
    # 清理并编译
    if mvn clean compile -f "$pom_file" -q; then
        print_success "Project compiled successfully"
    else
        print_error "Failed to compile project"
        exit 1
    fi
    
    # 打包
    print_info "Packaging executable JAR..."
    if mvn package -f "$pom_file" -DskipTests -q; then
        print_success "JAR packaged successfully"
    else
        print_error "Failed to package JAR"
        exit 1
    fi
}

# 查找生成的JAR文件
find_jar() {
    local jar_file=""
    
    # 查找可执行JAR
    if [ -f "target/tapdata-test-1.0-SNAPSHOT.jar" ]; then
        jar_file="target/tapdata-test-1.0-SNAPSHOT.jar"
    elif [ -f "target/tapdata-test-1.0-SNAPSHOT-shaded.jar" ]; then
        jar_file="target/tapdata-test-1.0-SNAPSHOT-shaded.jar"
    else
        # 查找任何JAR文件
        jar_file=$(find target -name "*.jar" -not -name "*sources.jar" | head -n 1)
    fi
    
    if [ -z "$jar_file" ]; then
        print_error "No JAR file found in target directory"
        exit 1
    fi
    
    print_info "Found JAR file: $jar_file"
    echo "$jar_file"
}

# 运行JAR文件
run_jar() {
    local jar_file=$1
    
    print_info "Running JAR file: $jar_file"
    print_info "Note: This JAR should run without additional JVM arguments on JDK 17+"
    
    # 简单的JVM参数（JDK 17不需要模块参数）
    JVM_ARGS=""
    JVM_ARGS="$JVM_ARGS -Xms512m -Xmx2g"
    JVM_ARGS="$JVM_ARGS -XX:+UseG1GC"
    
    # 日志配置
    if [ "$VERBOSE" = "true" ]; then
        JVM_ARGS="$JVM_ARGS -Dlogback.configurationFile=logback.xml"
    else
        JVM_ARGS="$JVM_ARGS -Dlogback.configurationFile=logback-simple.xml"
    fi
    
    print_info "JVM Args: $JVM_ARGS"
    
    java $JVM_ARGS -jar "$jar_file"
}

# 显示帮助信息
show_help() {
    echo "TapData Connector Tester - Build Executable JAR"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help        Show this help message"
    echo "  -b, --build-only  Only build, don't run"
    echo "  -r, --run-only    Only run existing JAR, don't build"
    echo "  -s, --shade       Use Maven Shade plugin POM"
    echo "  -j, --jdk17       Use JDK 17 specific POM"
    echo "  -v, --verbose     Enable verbose logging"
    echo ""
    echo "Environment Variables:"
    echo "  USE_SHADE=true    Use Maven Shade plugin"
    echo "  USE_JDK17=true    Use JDK 17 specific POM"
    echo "  VERBOSE=true      Enable verbose logging"
    echo ""
    echo "Examples:"
    echo "  $0                # Build and run with default settings"
    echo "  $0 --shade        # Build with Maven Shade plugin"
    echo "  $0 --build-only   # Only build JAR"
    echo "  $0 --run-only     # Only run existing JAR"
    echo "  $0 --verbose      # Run with verbose logging"
}

# 主函数
main() {
    local BUILD_ONLY=false
    local RUN_ONLY=false
    
    # 解析命令行参数
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -b|--build-only)
                BUILD_ONLY=true
                shift
                ;;
            -r|--run-only)
                RUN_ONLY=true
                shift
                ;;
            -s|--shade)
                USE_SHADE=true
                shift
                ;;
            -j|--jdk17)
                USE_JDK17=true
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    print_info "=== TapData Connector Tester - JAR Builder ==="
    
    # 检查环境
    check_java
    check_maven
    
    if [ "$RUN_ONLY" != "true" ]; then
        # 构建项目
        pom_file=$(select_pom)
        build_project "$pom_file"
    fi
    
    if [ "$BUILD_ONLY" != "true" ]; then
        # 运行JAR
        jar_file=$(find_jar)
        run_jar "$jar_file"
    else
        jar_file=$(find_jar)
        print_success "Build completed! JAR file: $jar_file"
        print_info "To run: java -jar $jar_file"
    fi
}

# 捕获中断信号
trap 'print_info "Build interrupted"; exit 1' INT TERM

# 运行主函数
main "$@"
