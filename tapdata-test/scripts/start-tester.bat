@echo off
setlocal enabledelayedexpansion

REM TapData Connector Performance Tester 启动脚本 (Windows)

title TapData Connector Performance Tester

REM 设置颜色（Windows 10+）
if not defined NO_COLOR (
    for /F %%a in ('echo prompt $E ^| cmd') do set "ESC=%%a"
    set "RED=!ESC![31m"
    set "GREEN=!ESC![32m"
    set "YELLOW=!ESC![33m"
    set "BLUE=!ESC![34m"
    set "NC=!ESC![0m"
) else (
    set "RED="
    set "GREEN="
    set "YELLOW="
    set "BLUE="
    set "NC="
)

REM 打印带颜色的消息
:print_info
echo !BLUE![INFO]!NC! %~1
goto :eof

:print_success
echo !GREEN![SUCCESS]!NC! %~1
goto :eof

:print_warning
echo !YELLOW![WARNING]!NC! %~1
goto :eof

:print_error
echo !RED![ERROR]!NC! %~1
goto :eof

REM 检查Java环境
:check_java
java -version >nul 2>&1
if errorlevel 1 (
    call :print_error "Java is not installed or not in PATH"
    pause
    exit /b 1
)

for /f "tokens=3" %%g in ('java -version 2^>^&1 ^| findstr /i "version"') do (
    set JAVA_VERSION=%%g
    set JAVA_VERSION=!JAVA_VERSION:"=!
)
call :print_info "Java version: !JAVA_VERSION!"
goto :eof

REM 检查Maven环境
:check_maven
mvn -version >nul 2>&1
if errorlevel 1 (
    call :print_error "Maven is not installed or not in PATH"
    pause
    exit /b 1
)

for /f "tokens=*" %%g in ('mvn -version 2^>^&1 ^| findstr "Apache Maven"') do (
    call :print_info "Maven version: %%g"
)
goto :eof

REM 编译项目
:compile_project
call :print_info "Compiling project..."

cd /d "%~dp0\.."
if errorlevel 1 (
    call :print_error "Failed to change directory"
    pause
    exit /b 1
)

mvn clean compile -q
if errorlevel 1 (
    call :print_error "Failed to compile project"
    pause
    exit /b 1
) else (
    call :print_success "Project compiled successfully"
)
goto :eof

REM 设置JVM参数
:setup_jvm_args
set JVM_ARGS=-Xms512m -Xmx2g
set JVM_ARGS=%JVM_ARGS% -XX:+UseG1GC -XX:MaxGCPauseMillis=200
set JVM_ARGS=%JVM_ARGS% -Dtapdata.log.level=INFO

if "%DEBUG%"=="true" (
    set JVM_ARGS=%JVM_ARGS% -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
    call :print_info "Debug mode enabled on port 5005"
)

set MAVEN_OPTS=%JVM_ARGS%
goto :eof

REM 启动测试器
:start_tester
call :print_info "Starting TapData Connector Performance Tester..."
call :print_info "JVM Args: %MAVEN_OPTS%"

mvn exec:java -Dexec.mainClass="io.tapdata.test.connector.ConnectorTesterMain" -q
goto :eof

REM 显示帮助信息
:show_help
echo TapData Connector Performance Tester
echo.
echo Usage: %~nx0 [OPTIONS]
echo.
echo Options:
echo   /h, /help      Show this help message
echo   /d, /debug     Enable debug mode
echo   /c, /compile   Force recompile before starting
echo   /skip-compile  Skip compilation step
echo.
echo Environment Variables:
echo   DEBUG=true     Enable debug mode
echo   JAVA_HOME      Java installation directory
echo   MAVEN_HOME     Maven installation directory
echo.
echo Examples:
echo   %~nx0                    # Start with default settings
echo   %~nx0 /debug             # Start with debug mode
echo   %~nx0 /compile           # Force recompile and start
echo   set DEBUG=true ^& %~nx0   # Start with debug mode via env var
goto :eof

REM 主函数
:main
set FORCE_COMPILE=false
set SKIP_COMPILE=false

REM 解析命令行参数
:parse_args
if "%~1"=="" goto :args_done
if /i "%~1"=="/h" goto :show_help_and_exit
if /i "%~1"=="/help" goto :show_help_and_exit
if /i "%~1"=="/d" (
    set DEBUG=true
    shift
    goto :parse_args
)
if /i "%~1"=="/debug" (
    set DEBUG=true
    shift
    goto :parse_args
)
if /i "%~1"=="/c" (
    set FORCE_COMPILE=true
    shift
    goto :parse_args
)
if /i "%~1"=="/compile" (
    set FORCE_COMPILE=true
    shift
    goto :parse_args
)
if /i "%~1"=="/skip-compile" (
    set SKIP_COMPILE=true
    shift
    goto :parse_args
)
call :print_error "Unknown option: %~1"
call :show_help
pause
exit /b 1

:show_help_and_exit
call :show_help
exit /b 0

:args_done
call :print_info "=== TapData Connector Performance Tester ==="

REM 检查环境
call :check_java
if errorlevel 1 exit /b 1

call :check_maven
if errorlevel 1 exit /b 1

REM 设置JVM参数
call :setup_jvm_args

REM 编译项目
if "%SKIP_COMPILE%"=="true" (
    call :print_info "Skipping compilation as requested"
) else (
    if "%FORCE_COMPILE%"=="true" (
        call :compile_project
        if errorlevel 1 exit /b 1
    ) else (
        if not exist "target\classes" (
            call :compile_project
            if errorlevel 1 exit /b 1
        ) else (
            call :print_info "Skipping compilation (target\classes exists)"
        )
    )
)

REM 启动测试器
call :start_tester
goto :eof

REM 程序入口
call :main %*
