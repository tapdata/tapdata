#!/bin/bash

# TapData Connector Performance Tester 使用示例脚本

# 设置颜色输出
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}=== TapData Connector Performance Tester 使用示例 ===${NC}"

# 示例1: 测试MySQL连接器
echo -e "\n${GREEN}示例1: 测试MySQL连接器${NC}"
echo "假设您有一个MySQL连接器JAR文件在 /path/to/mysql-connector.jar"
echo ""
echo "1. 启动测试器:"
echo "   ./start-tester.sh"
echo ""
echo "2. 在测试器中执行以下命令:"
echo -e "${YELLOW}"
cat << 'EOF'
# 加载MySQL连接器
load mysql /path/to/mysql-connector.jar

# 或者使用配置文件
load mysql /path/to/mysql-connector.jar mysql-config.json

# 测试连接
test-connection mysql

# 测试批量读取 (表名: users, 批量大小: 1000, 最大记录数: 10000)
test-batch-read mysql users 1000 10000

# 测试流式读取 (表名: users, 持续时间: 30秒)
test-stream-read mysql users 30000

# 测试写入性能 (表名: users, 记录数: 1000)
test-write mysql users 1000

# 运行完整基准测试
benchmark mysql users

# 启动性能监控 (每5秒监控一次)
monitor mysql 5
EOF
echo -e "${NC}"

# 示例2: 对比测试
echo -e "\n${GREEN}示例2: 对比不同连接器性能${NC}"
echo -e "${YELLOW}"
cat << 'EOF'
# 加载多个连接器
load mysql /path/to/mysql-connector.jar
load postgres /path/to/postgres-connector.jar

# 对比批量读取性能
test-batch-read mysql users 1000 10000
test-batch-read postgres users 1000 10000

# 对比写入性能
test-write mysql users 1000
test-write postgres users 1000

# 运行基准测试对比
benchmark mysql users
benchmark postgres users
EOF
echo -e "${NC}"

# 示例3: 开发调试
echo -e "\n${GREEN}示例3: 连接器开发调试${NC}"
echo "在开发连接器时，可以快速测试和调试:"
echo -e "${YELLOW}"
cat << 'EOF'
# 加载开发中的连接器
load my-dev-connector /path/to/my-connector-dev.jar

# 快速连接测试
test-connection my-dev-connector

# 小批量测试
test-batch-read my-dev-connector test_table 10 100

# 修改代码后重新加载 (热重载)
load my-dev-connector /path/to/my-connector-dev.jar

# 再次测试
test-batch-read my-dev-connector test_table 10 100
EOF
echo -e "${NC}"

# 示例4: 压力测试
echo -e "\n${GREEN}示例4: 大规模压力测试${NC}"
echo -e "${YELLOW}"
cat << 'EOF'
# 大批量读取测试
test-batch-read mysql large_table 10000 1000000

# 大批量写入测试
test-write mysql large_table 100000

# 长时间流式读取测试
test-stream-read mysql large_table 300000  # 5分钟
EOF
echo -e "${NC}"

# 配置文件示例
echo -e "\n${GREEN}配置文件示例${NC}"
echo "MySQL配置文件 (mysql-config.json):"
echo -e "${YELLOW}"
cat << 'EOF'
{
  "host": "localhost",
  "port": 3306,
  "database": "test",
  "username": "root",
  "password": "password",
  "timezone": "+08:00",
  "connectionTimeout": 30000,
  "maxPoolSize": 10
}
EOF
echo -e "${NC}"

echo ""
echo "PostgreSQL配置文件 (postgres-config.json):"
echo -e "${YELLOW}"
cat << 'EOF'
{
  "host": "localhost",
  "port": 5432,
  "database": "postgres",
  "schema": "public",
  "username": "postgres",
  "password": "password",
  "logDecoder": "wal2json",
  "timezone": "+08:00"
}
EOF
echo -e "${NC}"

# 性能调优建议
echo -e "\n${GREEN}性能调优建议${NC}"
echo "1. JVM内存设置:"
echo "   export MAVEN_OPTS=\"-Xms1g -Xmx4g\""
echo ""
echo "2. 启用调试模式:"
echo "   ./start-tester.sh --debug"
echo ""
echo "3. 强制重新编译:"
echo "   ./start-tester.sh --compile"
echo ""
echo "4. 跳过编译步骤:"
echo "   ./start-tester.sh --skip-compile"

# 常见问题
echo -e "\n${GREEN}常见问题解决${NC}"
echo "1. 连接器加载失败:"
echo "   - 检查JAR文件路径是否正确"
echo "   - 确认JAR文件包含连接器规范"
echo ""
echo "2. 连接测试失败:"
echo "   - 检查数据库服务是否运行"
echo "   - 验证连接配置参数"
echo "   - 检查网络连通性"
echo ""
echo "3. 性能测试超时:"
echo "   - 减少测试数据量"
echo "   - 检查数据库性能"
echo "   - 增加超时时间设置"

echo -e "\n${BLUE}更多信息请参考: README_HotLoadConnectorTester.md${NC}"
