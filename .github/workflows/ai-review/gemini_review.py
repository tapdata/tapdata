#!/usr/bin/env python3
"""
Gemini AI Code Review Script for Java Projects
Focus on resource leaks: memory, threads, TCP connections, file handles
"""

import os
import sys
import argparse
import google.generativeai as genai
from github import Github

# 代码审查提示词模板
REVIEW_PROMPT = """你是一位资深的 Java 代码审查专家，请对以下 Java 代码进行深度安全和质量审查。

**重点关注以下资源泄露问题：**

1. **内存泄露 (Memory Leaks)**
   - 静态集合持有对象引用导致无法 GC
   - 未关闭的资源（InputStream, OutputStream, Reader, Writer）
   - 监听器和回调未注销
   - ThreadLocal 使用后未清理
   - 内部类持有外部类引用导致的泄露
   - 缓存未设置过期策略或上限
   - 长生命周期对象持有短生命周期对象引用

2. **线程泄露 (Thread Leaks)**
   - 线程池未正确关闭（缺少 shutdown/shutdownNow）
   - 使用 new Thread() 创建线程但未管理生命周期
   - ExecutorService 未调用 shutdown()
   - ScheduledExecutorService 未停止
   - 定时任务未取消
   - 线程中断未正确处理

3. **TCP 连接泄露 (Connection Leaks)**
   - HttpClient/HttpURLConnection 未关闭或 disconnect
   - Socket 连接未关闭
   - 数据库连接未归还连接池（Connection 未关闭）
   - Redis/缓存连接未释放
   - RPC 客户端连接未关闭
   - 连接池配置不当（超时、最大连接数）
   - 连接超时未设置

4. **文件句柄泄露 (File Handle Leaks)**
   - FileInputStream/FileOutputStream 未关闭
   - RandomAccessFile 未关闭
   - FileChannel 未关闭
   - BufferedReader/BufferedWriter 未关闭
   - 未使用 try-with-resources（Java 7+）
   - Files.newInputStream/newOutputStream 未正确关闭
   - ZipInputStream/ZipOutputStream 未关闭

**额外审查要点：**
- 异常处理是否会导致资源未释放
- finally 块中的资源关闭顺序是否正确
- 是否有嵌套的资源需要关闭
- 是否使用了对象池但未归还对象
- 是否有循环引用或强引用导致无法 GC
- 并发场景下的资源竞争和泄露

**审查标准：**
✅ 优先推荐使用 try-with-resources（适用于所有 AutoCloseable 资源）
✅ 检查资源关闭的异常安全性
✅ 检查是否在所有分支路径都正确关闭资源
✅ 验证资源关闭顺序（先打开的后关闭）
✅ 检查连接池、线程池的配置合理性

**输出格式要求：**
请以 Markdown 格式输出审查结果，严格按照以下结构：

### 🔍 代码审查总结
[一句话总结代码整体质量和主要问题]

### 🚨 严重问题（必须修复）
[如果没有严重问题，输出：✅ 未发现严重问题]

每个严重问题格式：
**问题 X: [问题类型] - 第 [行号] 行**
- **问题描述**：[详细描述问题]
- **影响**：[可能导致的后果]
- **修复建议**：
```java
// 修复前
[原代码片段]

// 修复后
[修复后的代码]
```

### ⚠️ 潜在风险（建议修复）
[如果没有潜在风险，输出：✅ 未发现潜在风险]

格式同上。

### 💡 改进建议（最佳实践）
[如果没有改进建议，输出：✅ 代码符合最佳实践]

格式：
- **建议 X**：[建议内容]
  ```java
  // 示例代码
  ```

---

**待审查的代码：**

文件路径：`{filename}`

```java
{code}
```

请立即开始审查，输出必须严格遵循上述 Markdown 格式。
"""


def get_file_content(repo, filepath, ref):
    """获取文件内容"""
    try:
        content = repo.get_contents(filepath, ref=ref)
        return content.decoded_content.decode('utf-8')
    except Exception as e:
        print(f"❌ 获取文件失败 {filepath}: {e}")
        return None


def review_code_with_gemini(filename, code_content, api_key):
    """使用 Gemini AI 审查代码"""
    try:
        # 配置 Gemini API
        genai.configure(api_key=api_key)

        # 使用 Gemini 1.5 Pro 模型（推荐用于代码审查）
        try:
            model = genai.GenerativeModel('gemini-1.5-pro')  # ✅ 稳定版本
        except:
            try:
                model = genai.GenerativeModel('gemini-1.5-flash')  # ✅ 备选方案
            except:
                model = genai.GenerativeModel('gemini-pro')  # ✅ 兜底方案

        # 构建提示词
        prompt = REVIEW_PROMPT.format(
            filename=filename,
            code=code_content
        )

        # 生成配置
        generation_config = {
            'temperature': 0.1,  # 降低随机性，使输出更确定
            'top_p': 0.95,
            'top_k': 40,
            'max_output_tokens': 8192,
        }

        # 安全设置（允许代码相关讨论）
        safety_settings = [
            {
                "category": "HARM_CATEGORY_HARASSMENT",
                "threshold": "BLOCK_NONE"
            },
            {
                "category": "HARM_CATEGORY_HATE_SPEECH",
                "threshold": "BLOCK_NONE"
            },
            {
                "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                "threshold": "BLOCK_NONE"
            },
            {
                "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                "threshold": "BLOCK_NONE"
            },
        ]

        print(f"  📤 发送代码到 Gemini AI...")

        # 调用 Gemini API
        response = model.generate_content(
            prompt,
            generation_config=generation_config,
            safety_settings=safety_settings
        )

        print(f"  ✅ 审查完成")

        return response.text

    except Exception as e:
        print(f"❌ Gemini AI 审查失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def post_review_comment(github_token, repo_name, pr_number, review_results):
    """将审查结果发布到 PR"""
    try:
        g = Github(github_token)
        repo = g.get_repo(repo_name)
        pr = repo.get_pull(pr_number)

        # 构建评论内容
        comment_body = f"""## 🤖 Gemini AI 代码审查报告（资源泄露检测）

本次审查由 **Google Gemini 1.5 Pro** 提供支持

**审查重点**：内存泄露 · 线程泄露 · TCP连接泄露 · 文件句柄泄露

---

{review_results}

---

<details>
<summary>📚 审查说明</summary>

### 本审查关注的问题类型：

#### 🧠 内存泄露
- 静态集合无限增长
- ThreadLocal 未清理
- 监听器未注销
- 缓存无过期策略

#### 🧵 线程泄露
- 线程池未关闭
- 定时任务未取消
- 手动线程未管理

#### 🔌 连接泄露
- HTTP/Socket 连接未关闭
- 数据库连接未归还
- 连接池配置不当

#### 📁 文件句柄泄露
- 文件流未关闭
- 未使用 try-with-resources
- 资源关闭异常处理不当

### 审查建议等级：
- 🚨 **严重问题**：必须修复，可能导致生产环境故障
- ⚠️ **潜在风险**：建议修复，在特定场景下可能出问题
- 💡 **改进建议**：最佳实践，提升代码质量

**注意**：AI 审查结果仅供参考，请结合实际业务场景人工复核。

</details>

---
<sub>Powered by Google Gemini 1.5 Pro | [Gemini API](https://ai.google.dev/)</sub>
"""

        pr.create_issue_comment(comment_body)
        print("✅ 审查结果已发布到 PR")

    except Exception as e:
        print(f"❌ 发布评论失败: {e}")
        import traceback
        traceback.print_exc()
        raise


def main():
    parser = argparse.ArgumentParser(description='Gemini AI Code Review for Java')
    parser.add_argument('--pr-number', type=int, required=True, help='PR number')
    parser.add_argument('--repo', type=str, required=True, help='Repository name (owner/repo)')
    parser.add_argument('--files', type=str, required=True, help='Changed files (space-separated)')

    args = parser.parse_args()

    # 获取环境变量
    gemini_api_key = os.getenv('GEMINI_API_KEY')
    github_token = os.getenv('GITHUB_TOKEN')

    if not gemini_api_key:
        print("❌ 错误: 未设置 GEMINI_API_KEY")
        print("请在 GitHub Secrets 中添加 GEMINI_API_KEY")
        sys.exit(1)

    if not github_token:
        print("❌ 错误: 未设置 GITHUB_TOKEN")
        sys.exit(1)

    # 初始化 GitHub
    g = Github(github_token)
    repo = g.get_repo(args.repo)
    pr = repo.get_pull(args.pr_number)

    # 获取变更的文件
    changed_files = args.files.split()

    print(f"\n{'='*60}")
    print(f"🚀 Gemini AI 代码审查开始")
    print(f"{'='*60}")
    print(f"📋 PR 编号: #{args.pr_number}")
    print(f"📂 仓库: {args.repo}")
    print(f"📁 变更文件数: {len(changed_files)}")
    print(f"{'='*60}\n")

    all_reviews = []
    reviewed_count = 0
    skipped_count = 0

    for filepath in changed_files:
        if not filepath.endswith('.java'):
            continue

        print(f"🔍 正在审查: {filepath}")

        # 获取文件内容
        code_content = get_file_content(repo, filepath, pr.head.sha)
        if not code_content:
            skipped_count += 1
            continue

        # 检查文件大小
        code_size = len(code_content)
        if code_size > 100000:  # 100KB 限制
            print(f"  ⚠️  文件过大 ({code_size} bytes)，跳过")
            skipped_count += 1
            continue

        print(f"  📏 文件大小: {code_size} bytes")

        # Gemini AI 审查
        review_result = review_code_with_gemini(filepath, code_content, gemini_api_key)

        if review_result:
            all_reviews.append(f"### 📄 `{filepath}`\n\n{review_result}\n")
            reviewed_count += 1
            print(f"  ✅ 审查成功\n")
        else:
            skipped_count += 1
            print(f"  ❌ 审查失败\n")

    print(f"{'='*60}")
    print(f"📊 审查统计:")
    print(f"  ✅ 成功审查: {reviewed_count} 个文件")
    print(f"  ⏭️  跳过: {skipped_count} 个文件")
    print(f"{'='*60}\n")

    if all_reviews:
        # 合并所有审查结果
        final_review = "\n".join(all_reviews)

        # 发布到 PR
        print("📤 正在发布审查结果到 PR...")
        post_review_comment(github_token, args.repo, args.pr_number, final_review)
        print("\n✅ 代码审查完成！")
        print(f"🔗 查看 PR: https://github.com/{args.repo}/pull/{args.pr_number}")
    else:
        print("\n⚠️  没有成功审查任何 Java 文件")
        sys.exit(1)


if __name__ == "__main__":
    main()