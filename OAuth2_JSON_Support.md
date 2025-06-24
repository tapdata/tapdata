# OAuth2 Token端点JSON格式支持

## 概述

现在OAuth2的`/oauth/token`端点支持两种Content-Type格式：

1. **application/x-www-form-urlencoded** (原有格式，优先支持)
2. **application/json** (新增格式)

## 实现方式

通过添加`OAuth2JsonSupportFilter`过滤器，在Spring Security OAuth2处理请求之前拦截JSON格式的请求，并将其转换为标准的form-urlencoded格式。

### 关键组件

1. **OAuth2JsonSupportFilter**: 核心过滤器，负责JSON到form格式的转换
2. **AuthorizationConfig**: 修改了OAuth2安全配置，添加了自定义过滤器

## 使用方式

### 原有格式 (application/x-www-form-urlencoded)

```bash
curl -X POST "http://127.0.0.1:13030/oauth/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=68376c8a4cf9b840dba2e118" \
  -d "client_secret=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" \
  -v
```

### 新增格式 (application/json)

```bash
curl "http://127.0.0.1:13030/oauth/token" \
  -H "Content-Type: application/json" \
  -d '{
    "grant_type": "client_credentials",
    "client_id": "68376c8a4cf9b840dba2e118",
    "client_secret": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
  }'
```

## 兼容性

- **向后兼容**: 完全保持对原有`application/x-www-form-urlencoded`格式的支持
- **优先级**: 原有格式具有更高的优先级，系统会优先处理form格式的请求
- **错误处理**: 对于无效的JSON格式或缺少必需参数的请求，会返回标准的OAuth2错误响应

## 支持的Grant Types

目前主要支持以下grant types：

1. **client_credentials**: 客户端凭证模式
   - 必需参数: `grant_type`, `client_id`, `client_secret`
   - 可选参数: `scope`

## 响应格式

无论使用哪种请求格式，响应都是标准的OAuth2 JSON格式：

### 成功响应
```json
{
  "access_token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "Bearer",
  "expires_in": 1209600,
  "scope": "read write"
}
```

### 错误响应
```json
{
  "error": "invalid_request",
  "error_description": "Missing required parameter: grant_type"
}
```

## 技术细节

### 过滤器工作流程

1. 拦截所有发往`/oauth/token`的POST请求
2. 检查Content-Type头
3. 如果是`application/json`:
   - 解析JSON请求体
   - 验证必需参数
   - 转换为form参数格式
   - 创建包装的HttpServletRequest
   - 传递给Spring Security OAuth2处理器
4. 如果是`application/x-www-form-urlencoded`或其他格式:
   - 直接传递给下一个过滤器

### 错误处理

- JSON解析错误: 返回`invalid_request`错误
- 缺少必需参数: 返回`invalid_request`错误，包含具体的错误描述
- 不支持的Content-Type: 返回`unsupported_media_type`错误

## 配置

过滤器已自动配置在OAuth2授权服务器的安全过滤器链中，无需额外配置。

## 日志

过滤器使用SLF4J进行日志记录，可以通过调整日志级别来查看详细的处理信息：

```properties
logging.level.com.tapdata.tm.oauth2.filter.OAuth2JsonSupportFilter=DEBUG
```

## 测试

可以使用以下命令测试两种格式：

```bash
# 测试form格式（原有格式）
curl -X POST "http://127.0.0.1:13030/oauth/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=68376c8a4cf9b840dba2e118&client_secret=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" \
  -v

# 测试JSON格式（新增格式）
curl -X POST "http://127.0.0.1:13030/oauth/token" \
  -H "Content-Type: application/json" \
  -d '{"grant_type":"client_credentials","client_id":"68376c8a4cf9b840dba2e118","client_secret":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"}' \
  -v
```

两种请求应该返回相同的结果。

## 实现细节

### 文件修改列表

1. **新增文件**：
   - `manager/tm/src/main/java/com/tapdata/tm/oauth2/filter/OAuth2JsonSupportFilter.java` - 核心过滤器
   - `manager/tm/src/main/java/com/tapdata/tm/config/OAuth2FilterConfig.java` - 过滤器配置

2. **修改文件**：
   - `manager/tm/src/main/java/com/tapdata/tm/config/AuthorizationConfig.java` - 添加必要的import

### 过滤器注册方式

使用`FilterRegistrationBean`来注册过滤器，避免了Spring Security过滤器链顺序的问题：

```java
@Bean
public FilterRegistrationBean<OAuth2JsonSupportFilter> oAuth2JsonSupportFilterRegistration() {
    FilterRegistrationBean<OAuth2JsonSupportFilter> registration = new FilterRegistrationBean<>();
    registration.setFilter(new OAuth2JsonSupportFilter());
    registration.addUrlPatterns("/oauth/token");
    registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
    registration.setName("OAuth2JsonSupportFilter");
    return registration;
}
```
