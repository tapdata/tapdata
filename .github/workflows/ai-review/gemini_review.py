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

2. **线程泄露 (Thread Leaks)**
   - 线程池未正确关闭（缺少 shutdown/shutdownNow）
   - 使用 new Thread() 创建线程但未管理生命周期
   - ExecutorService 未调用 shutdown()
   - ScheduledExecutorService 未停止
   - 定时任务未取消

3. **TCP 连接泄露 (Connection Leaks)**
   - HttpClient/HttpURLConnection 未关闭或 disconnect
   - Socket 连接未关闭
   - 数据库连接未归还连接池
   - Redis/缓存连接未释放
   - RPC 客户端连接未关闭

4. **文件句柄泄露 (File Handle Leaks)**
   - FileInputStream/FileOutputStream 未关闭
   - RandomAccessFile 未关闭
   - BufferedReader/BufferedWriter 未关闭
   - 未使用 try-with-resources（Java 7+）

**输出格式要求：**
请以 Markdown 格式输出审查结果，包括：

### 🔍 代码审查总结
[一句话总结]

### 🚨 严重问题（必须修复）
[如果没有，输出：✅ 未发现严重问题]

### ⚠️ 潜在风险（建议修复）
[如果没有，输出：✅ 未发现潜在风险]

### 💡 改进建议（最佳实践）
[如果没有，输出：✅ 代码符合最佳实践]

---

**待审查的代码：**

文件路径：`{filename}`

```java
{code}
```

请立即开始审查。
"""


def list_available_models(api_key):
    """列出所有可用的模型"""
    try:
        genai.configure(api_key=api_key)
        available_models = []

        print("  🔍 正在检测可用的 Gemini 模型...")

        for model in genai.list_models():
            if 'generateContent' in model.supported_generation_methods:
                # 移除 'models/' 前缀（如果有）
                model_name = model.name.replace('models/', '')
                available_models.append(model_name)
                print(f"    ✅ 发现可用模型: {model_name}")

        return available_models
    except Exception as e:
        print(f"  ⚠️ 无法列出模型: {e}")
        return []


def get_best_model(api_key):
    """自动选择最佳可用模型"""
    genai.configure(api_key=api_key)

    # 优先级列表（从高到低）
    preferred_models = [
        'gemini-1.5-pro-002',
        'gemini-1.5-pro-001',
        'gemini-1.5-pro',
        'gemini-1.5-flash-002',
        'gemini-1.5-flash-001',
        'gemini-1.5-flash',
        'gemini-1.5-flash-8b',
        'gemini-pro',
        'gemini-1.0-pro',
    ]

    # 获取可用模型
    available_models = list_available_models(api_key)

    if not available_models:
        print("  ⚠️ 无法获取可用模型列表，使用默认模型尝试")
        # 尝试最常见的模型
        for model_name in ['gemini-pro', 'gemini-1.0-pro']:
            try:
                model = genai.GenerativeModel(model_name)
                print(f"  ✅ 使用模型: {model_name}")
                return model, model_name
            except:
                continue
        raise Exception("无法找到任何可用的 Gemini 模型")

    # 从优先级列表中选择第一个可用的模型
    for preferred in preferred_models:
        if preferred in available_models:
            try:
                model = genai.GenerativeModel(preferred)
                print(f"  ✅ 选择最佳模型: {preferred}")
                return model, preferred
            except Exception as e:
                print(f"  ⚠️ 无法加载 {preferred}: {e}")
                continue

    # 如果优先级列表中没有可用的，使用找到的第一个
    if available_models:
        model_name = available_models[0]
        try:
            model = genai.GenerativeModel(model_name)
            print(f"  ✅ 使用可用模型: {model_name}")
            return model, model_name
        except Exception as e:
            print(f"  ❌ 无法加载 {model_name}: {e}")

    raise Exception(f"无法加载任何 Gemini 模型。可用模型: {available_models}")


def get_file_content(repo, filepath, ref):
    """获取文件内容"""
    try:
        content = repo.get_contents(filepath, ref=ref)
        return content.decoded_content.decode('utf-8')
    except Exception as e:
        print(f"❌ 获取文件失败 {filepath}: {e}")
        return None


def review_code_with_gemini(filename, code_content, api_key, model_cache=None):
    """使用 Gemini AI 审查代码"""
    try:
        # 使用缓存的模型或获取新模型
        if model_cache and 'model' in model_cache:
            model = model_cache['model']
            model_name = model_cache['name']
        else:
            model, model_name = get_best_model(api_key)
            if model_cache is not None:
                model_cache['model'] = model
                model_cache['name'] = model_name

        prompt = REVIEW_PROMPT.format(
            filename=filename,
            code=code_content
        )

        generation_config = {
            'temperature': 0.1,
            'top_p': 0.95,
            'top_k': 40,
            'max_output_tokens': 8192,
        }

        safety_settings = [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
        ]

        print(f"  📤 发送代码到 Gemini AI (模型: {model_name})...")

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


def post_review_comment(github_token, repo_name, pr_number, review_results, model_name):
    """将审查结果发布到 PR"""
    try:
        g = Github(github_token)
        repo = g.get_repo(repo_name)
        pr = repo.get_pull(pr_number)

        comment_body = f"""## 🤖 Gemini AI 代码审查报告（资源泄露检测）

本次审查由 **Google Gemini AI** 提供支持（模型: `{model_name}`）

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

**注意**：AI 审查结果仅供参考，请结合实际业务场景人工复核。

</details>

---
<sub>Powered by Google Gemini AI ({model_name})</sub>
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

    gemini_api_key = os.getenv('GEMINI_API_KEY')
    github_token = os.getenv('GITHUB_TOKEN')

    if not gemini_api_key:
        print("❌ 错误: 未设置 GEMINI_API_KEY")
        sys.exit(1)

    if not github_token:
        print("❌ 错误: 未设置 GITHUB_TOKEN")
        sys.exit(1)

    g = Github(github_token)
    repo = g.get_repo(args.repo)
    pr = repo.get_pull(args.pr_number)

    changed_files = args.files.split()

    print(f"\n{'='*60}")
    print(f"🚀 Gemini AI 代码审查开始")
    print(f"{'='*60}")
    print(f"📋 PR 编号: #{args.pr_number}")
    print(f"📂 仓库: {args.repo}")
    print(f"📁 变更文件数: {len(changed_files)}")
    print(f"{'='*60}\n")

    # 模型缓存，避免重复检测
    model_cache = {}

    all_reviews = []
    reviewed_count = 0
    skipped_count = 0
    model_name = "unknown"

    for filepath in changed_files:
        if not filepath.endswith('.java'):
            continue

        print(f"🔍 正在审查: {filepath}")

        code_content = get_file_content(repo, filepath, pr.head.sha)
        if not code_content:
            skipped_count += 1
            continue

        code_size = len(code_content)
        if code_size > 100000:
            print(f"  ⚠️  文件过大 ({code_size} bytes)，跳过")
            skipped_count += 1
            continue

        print(f"  📏 文件大小: {code_size} bytes")

        review_result = review_code_with_gemini(filepath, code_content, gemini_api_key, model_cache)

        if review_result:
            all_reviews.append(f"### 📄 `{filepath}`\n\n{review_result}\n")
            reviewed_count += 1
            model_name = model_cache.get('name', 'unknown')
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
        final_review = "\n".join(all_reviews)
        print("📤 正在发布审查结果到 PR...")
        post_review_comment(github_token, args.repo, args.pr_number, final_review, model_name)
        print("\n✅ 代码审查完成！")
        print(f"🔗 查看 PR: https://github.com/{args.repo}/pull/{args.pr_number}")
    else:
        print("\n⚠️  没有成功审查任何 Java 文件")
        sys.exit(1)


if __name__ == "__main__":
    main()
