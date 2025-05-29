# OpenAPI Generator Resources

这个目录用于存放OpenAPI Generator相关的资源文件。

## 必需文件

### 1. openapi-generator-cli.jar

这是OpenAPI Generator的命令行工具JAR包，需要手动下载并放置在此目录中。

**下载地址**: https://github.com/OpenAPITools/openapi-generator/releases

**下载步骤**:
1. 访问上述GitHub releases页面
2. 选择最新版本（推荐使用稳定版本）
3. 下载 `openapi-generator-cli-{version}.jar` 文件
4. 将文件重命名为 `openapi-generator-cli.jar`
5. 放置在当前目录中

**示例下载命令**:
```bash
# 下载最新版本 (请替换为实际的最新版本号)
wget https://repo1.maven.org/maven2/org/openapitools/openapi-generator-cli/7.2.0/openapi-generator-cli-7.2.0.jar -O openapi-generator-cli.jar

# 或使用 curl
curl -L https://repo1.maven.org/maven2/org/openapitools/openapi-generator-cli/7.2.0/openapi-generator-cli-7.2.0.jar -o openapi-generator-cli.jar
```

### 2. 模板文件 (可选)

如果需要自定义代码生成模板，可以在此目录中创建模板文件夹。

**目录结构示例**:
```
openapi-generator/
├── openapi-generator-cli.jar
├── templates/
│   ├── java/
│   │   ├── api.mustache
│   │   ├── model.mustache
│   │   └── ...
│   ├── javascript/
│   │   ├── api.mustache
│   │   └── ...
│   └── ...
└── README.md
```

## 验证安装

下载完成后，可以通过以下命令验证JAR包是否正常工作：

```bash
java -jar openapi-generator-cli.jar version
```

应该会输出OpenAPI Generator的版本信息。

## 注意事项

1. 确保系统已安装Java 8或更高版本
2. JAR包文件大小通常在40-50MB左右
3. 建议定期更新到最新版本以获得最佳兼容性和功能
4. 如果使用自定义模板，请确保模板语法正确

## 支持的生成器

OpenAPI Generator支持50+种编程语言和框架，包括但不限于：

- **客户端**: Java, JavaScript, TypeScript, Python, C#, Go, PHP, Ruby, Swift, Kotlin, Dart, etc.
- **服务端**: Spring Boot, Node.js, ASP.NET Core, Go Gin, Flask, etc.
- **文档**: HTML, Markdown, AsciiDoc, etc.

查看完整列表：
```bash
java -jar openapi-generator-cli.jar list
```
