# OpenAPI Generator API

This is a code generation service based on OpenAPI Generator that can automatically generate SDK code for various programming languages based on OpenAPI specifications.

## Features

- Support for Java SDK generation only
- Java 17 template support with modern Java features
- Automatic compilation and download of generated code as JAR packages
- Support for custom package names, artifact-id, group-id and other parameters
- Provides RESTful API interface
- Supports both GET and POST request methods

## API接口

### 1. 生成代码 (POST)

**接口地址**: `POST /api/openapi/generator/generate`

**请求体**:
```json
{
  "oas": "https://example.com/api/openapi.json",
  "lan": "java",
  "packageName": "io.tapdata.sdk",
  "artifactId": "tapdata-sdk",
  "groupId": "io.tapdata"
}
```

**参数说明**:
- `oas`: OpenAPI规范文件的URL (必填)
- `lan`: 生成的编程语言，默认为"java"
- `packageName`: 生成代码的包名，默认为"io.tapdata.sdk"
- `artifactId`: Artifact ID，默认为"tapdata-sdk"
- `groupId`: Group ID，默认为"io.tapdata"

**响应**: 返回JAR文件下载

### 2. 生成代码 (GET)

**接口地址**: `GET /api/openapi/generator/generate`

**请求参数**:
- `oas`: OpenAPI规范文件的URL (必填)
- `lan`: 生成的编程语言，默认为"java"

**示例**:
```
GET /api/openapi/generator/generate?oas=https://example.com/api/openapi.json&lan=java
```

### 3. 获取支持的语言

**接口地址**: `GET /api/openapi/generator/languages`

**响应**:
```json
{
  "code": "ok",
  "data": ["java", "javascript", "typescript-node", "python", "csharp", "go", "php", "ruby", "swift", "kotlin", "scala", "dart"]
}
```

### 4. 健康检查

**接口地址**: `GET /api/openapi/generator/health`

**响应**:
```json
{
  "code": "ok",
  "data": "OpenAPI Generator服务运行正常"
}
```

## 配置说明

在 `application.properties` 或 `application.yml` 中添加以下配置：

```properties
# OpenAPI Generator JAR包路径
openapi.generator.jar.path=/path/to/openapi-generator-cli.jar

# 模板目录路径
openapi.generator.template.path=/path/to/templates

# 临时目录
openapi.generator.temp.dir=${java.io.tmpdir}
```

## 支持的编程语言

- Java (Java 17 with modern features)

## 使用示例

### curl 示例

```bash
# POST 请求
curl -X POST "http://localhost:3000/api/openapi/generator/generate" \
  -H "Content-Type: application/json" \
  -d '{
    "oas": "https://petstore3.swagger.io/api/v3/openapi.json",
    "lan": "java"
  }' \
  --output generated-sdk.zip

# GET 请求
curl "http://localhost:3000/api/openapi/generator/generate?oas=https://petstore3.swagger.io/api/v3/openapi.json&lan=java" \
  --output generated-sdk.zip
```

### JavaScript 示例

```javascript
// 使用 fetch API
fetch('/api/openapi/generator/generate', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({
    oas: 'https://petstore3.swagger.io/api/v3/openapi.json',
    lan: 'java'
  })
})
.then(response => response.blob())
.then(blob => {
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'generated-sdk.zip';
  a.click();
});
```

## 注意事项

1. 确保OpenAPI Generator CLI JAR文件存在且可执行
2. 确保系统已安装Maven并配置在PATH环境变量中
3. 确保有足够的磁盘空间用于临时文件和编译过程
4. 生成和编译大型SDK时可能需要较长时间，建议设置合适的超时时间
5. 临时文件会在生成完成后自动清理
6. 目前只支持Java语言，其他语言将返回错误信息

## 错误处理

API会返回标准的HTTP状态码：
- 200: 成功
- 400: 请求参数错误
- 500: 服务器内部错误

错误响应格式：
```json
{
  "code": "error",
  "message": "错误描述信息"
}
```
