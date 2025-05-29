# OpenAPI Generator 部署指南

## 部署前准备

### 1. 下载 OpenAPI Generator CLI

```bash
# 进入资源目录
cd manager/tm/src/main/resources/openapi-generator

# 下载最新版本的 OpenAPI Generator CLI
wget https://repo1.maven.org/maven2/org/openapitools/openapi-generator-cli/7.2.0/openapi-generator-cli-7.2.0.jar -O openapi-generator-cli.jar

# 验证下载
java -jar openapi-generator-cli.jar version
```

### 2. 配置应用属性

在 `application.properties` 或 `application.yml` 中添加以下配置：

```properties
# OpenAPI Generator 配置
openapi.generator.jar.path=classpath:openapi-generator/openapi-generator-cli.jar
openapi.generator.template.path=classpath:openapi-generator
openapi.generator.temp.dir=${java.io.tmpdir}

# 文件上传限制
spring.servlet.multipart.max-file-size=10MB
spring.servlet.multipart.max-request-size=10MB

# 请求超时
spring.mvc.async.request-timeout=300000
```

### 3. 确保Java环境

确保运行环境有Java 8或更高版本：

```bash
java -version
```

## 部署步骤

### 1. 编译项目

```bash
# 在项目根目录执行
mvn clean compile
```

### 2. 运行应用

```bash
# 启动Spring Boot应用
mvn spring-boot:run

# 或者打包后运行
mvn clean package
java -jar target/tm-*.jar
```

### 3. 验证部署

访问以下URL验证服务是否正常：

- 健康检查: `GET http://localhost:3000/api/openapi/generator/health`
- API文档: `http://localhost:3000/swagger-ui/index.html`

## 生产环境配置

### 1. 外部配置

创建外部配置文件 `application-prod.properties`：

```properties
# 生产环境配置
openapi.generator.jar.path=/opt/tapdata/openapi-generator/openapi-generator-cli.jar
openapi.generator.template.path=/opt/tapdata/openapi-generator/templates
openapi.generator.temp.dir=/tmp/tapdata-openapi

# 日志配置
logging.level.com.tapdata.tm.openapi.generator=INFO
logging.file.name=/var/log/tapdata/openapi-generator.log

# 性能优化
spring.mvc.async.request-timeout=600000
```

### 2. 系统服务配置

创建 systemd 服务文件 `/etc/systemd/system/tapdata-tm.service`：

```ini
[Unit]
Description=Tapdata TM Application
After=network.target

[Service]
Type=simple
User=tapdata
ExecStart=/usr/bin/java -jar /opt/tapdata/tm.jar --spring.profiles.active=prod
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### 3. 目录权限

```bash
# 创建必要目录
sudo mkdir -p /opt/tapdata/openapi-generator
sudo mkdir -p /var/log/tapdata
sudo mkdir -p /tmp/tapdata-openapi

# 设置权限
sudo chown -R tapdata:tapdata /opt/tapdata
sudo chown -R tapdata:tapdata /var/log/tapdata
sudo chown -R tapdata:tapdata /tmp/tapdata-openapi
```

## Docker 部署

### 1. Dockerfile

```dockerfile
FROM openjdk:17-jre-slim

# 安装必要工具
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# 创建应用目录
WORKDIR /app

# 复制应用文件
COPY target/tm-*.jar app.jar
COPY src/main/resources/openapi-generator /app/openapi-generator

# 下载 OpenAPI Generator CLI
RUN wget https://repo1.maven.org/maven2/org/openapitools/openapi-generator-cli/7.2.0/openapi-generator-cli-7.2.0.jar \
    -O /app/openapi-generator/openapi-generator-cli.jar

# 设置环境变量
ENV OPENAPI_GENERATOR_JAR_PATH=/app/openapi-generator/openapi-generator-cli.jar
ENV OPENAPI_GENERATOR_TEMPLATE_PATH=/app/openapi-generator
ENV OPENAPI_GENERATOR_TEMP_DIR=/tmp

# 暴露端口
EXPOSE 3000

# 启动应用
CMD ["java", "-jar", "app.jar"]
```

### 2. docker-compose.yml

```yaml
version: '3.8'
services:
  tapdata-tm:
    build: .
    ports:
      - "3000:3000"
    environment:
      - SPRING_PROFILES_ACTIVE=docker
      - OPENAPI_GENERATOR_JAR_PATH=/app/openapi-generator/openapi-generator-cli.jar
    volumes:
      - /tmp/tapdata-openapi:/tmp
    restart: unless-stopped
```

## 监控和维护

### 1. 健康检查

设置定期健康检查：

```bash
# 添加到 crontab
*/5 * * * * curl -f http://localhost:3000/api/openapi/generator/health || echo "Service down" | mail -s "Alert" admin@example.com
```

### 2. 日志监控

配置日志轮转：

```bash
# /etc/logrotate.d/tapdata
/var/log/tapdata/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 644 tapdata tapdata
}
```

### 3. 临时文件清理

设置定期清理临时文件：

```bash
# 添加到 crontab
0 2 * * * find /tmp/tapdata-openapi -type f -mtime +1 -delete
```

## 故障排除

### 1. 常见问题

- **JAR包未找到**: 检查 `openapi.generator.jar.path` 配置
- **权限问题**: 确保应用有读写临时目录的权限
- **内存不足**: 增加JVM堆内存 `-Xmx2g`
- **超时问题**: 调整 `spring.mvc.async.request-timeout`

### 2. 调试模式

启用调试日志：

```properties
logging.level.com.tapdata.tm.openapi.generator=DEBUG
logging.level.org.springframework.web=DEBUG
```

### 3. 性能优化

```properties
# JVM 参数
-Xms512m -Xmx2g -XX:+UseG1GC

# 线程池配置
spring.task.execution.pool.core-size=10
spring.task.execution.pool.max-size=50
```
