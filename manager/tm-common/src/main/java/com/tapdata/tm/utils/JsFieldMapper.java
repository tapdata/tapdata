package com.tapdata.tm.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 动态解析 JavaScript 脚本中 return 对象与 record 之间的字段映射关系。
 * 自动识别 return 返回的变量名或对象字面量，并解析其赋值来源。
 *
 * <p>当前实现是面向字段血缘推导的轻量级启发式解析，不是完整 JavaScript AST 解析器。
 * 支持的场景：
 * <ul>
 *     <li>第一个简单返回语句：{@code return ret;}。</li>
 *     <li>第一个返回语句直接返回简单对象字面量：{@code return { newField: record.oldField };}。</li>
 *     <li>{@code var/let/const ret = { newField: record.oldField }} 或 {@code ret = {...}} 对象字面量。</li>
 *     <li>{@code ret.newField = record.oldField} 形式的点号赋值。</li>
 *     <li>{@code ret.aliasField = ret.knownField} 形式的同对象字段转传，前提是来源字段已解析。</li>
 *     <li>{@code delete ret.fieldName} 会从最终映射中移除字段。</li>
 * </ul>
 *
 * <p>边界限制：
 * <ul>
 *     <li>字段名和变量名仅支持 {@code [a-zA-Z_][a-zA-Z0-9_]*} 形式。</li>
 *     <li>不支持括号访问、解构、展开、嵌套对象、数组、函数调用、多字段计算表达式。</li>
 *     <li>会忽略行注释和块注释中的文本，但不会做完整 JavaScript 语法解析；复杂/不可信脚本应使用真正的 JavaScript AST 解析。</li>
 *     <li>{@code delete} 语句按最终结果处理，不表达完整 JavaScript 执行顺序。</li>
 * </ul>
 */
public final class JsFieldMapper {
    private static final String IDENTIFIER = "[a-zA-Z_][a-zA-Z0-9_]*";

    private JsFieldMapper() {

    }

    /**
     * 从给定的 JavaScript 脚本中解析字段映射关系。
     * 先提取第一个 return 语句，再解析返回对象与 record 的赋值关系。
     *
     * @param jsCode 完整的 JavaScript 代码字符串
     * @return 映射表（新字段名 → 原字段名），若无映射则返回空 Map
     */
    public static Map<String, String> parseMapping(String jsCode) {
        Map<String, String> mapping = new HashMap<>();
        if (StringUtils.isBlank(jsCode)) {
            return mapping;
        }
        jsCode = stripComments(jsCode);
        ReturnExpression returnExpression = extractReturnExpression(jsCode);
        if (returnExpression == null) {
            return mapping;
        }

        // 1. 解析对象字面量，建立初始映射
        String returnedVar = returnExpression.variableName;
        if (returnExpression.objectLiteralBody != null) {
            mapping.putAll(parseObjectLiteral(returnExpression.objectLiteralBody));
            return mapping;
        }
        if (returnedVar == null) {
            return mapping;
        }
        String literalBody = extractObjectLiteral(jsCode, returnedVar);
        if (literalBody != null) {
            mapping.putAll(parseObjectLiteral(literalBody)); // 此时 mapping 包含 pymtMthdIntrlNamEng → record.pymt_mthd_intrl_nam_eng
        }

        // 2. 解析所有赋值语句，包括右侧为 returnedVar.xxx 的情况
        Pattern assignPattern = Pattern.compile(
                "\\b" + Pattern.quote(returnedVar) +
                        "\\.(" + IDENTIFIER + ")\\s*=\\s*(" +
                        "\\brecord\\." + IDENTIFIER + "|" +          // case 1: record.xxx
                        "\\b" + Pattern.quote(returnedVar) + "\\." + IDENTIFIER + // case 2: returnedVar.xxx
                        ")[ \\t\\f]*(?=;|\\r?\\n|$)"
        );
        Matcher matcher = assignPattern.matcher(jsCode);
        while (matcher.find()) {
            String newField = matcher.group(1);
            String rightSide = matcher.group(2);

            if (rightSide.startsWith("record.")) {
                // 直接来源
                String oldField = rightSide.substring(7); // 去掉 "record."
                mapping.put(newField, oldField);
            } else if (rightSide.startsWith(returnedVar + ".")) {
                // 自身属性传递：查找来源字段在 mapping 中的值
                String sourceField = rightSide.substring(returnedVar.length() + 1);
                if (mapping.containsKey(sourceField)) {
                    // 将左侧新字段指向 sourceField 原来的 record 来源
                    mapping.put(newField, mapping.get(sourceField));
                }
            }
        }

        // 3. 处理 delete 语句，移除被删除的字段（如果未被覆盖）
        Pattern deletePattern = Pattern.compile(
                "\\bdelete\\s+\\b" + Pattern.quote(returnedVar) + "\\.(" + IDENTIFIER + ")"
        );
        Matcher deleteMatcher = deletePattern.matcher(jsCode);
        while (deleteMatcher.find()) {
            String deletedField = deleteMatcher.group(1);
            mapping.remove(deletedField); // 移除被删字段的映射（如 pymtMthdIntrlNamEng）
        }

        return mapping;
    }

    private static String stripComments(String jsCode) {
        StringBuilder builder = new StringBuilder(jsCode.length());
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inTemplateLiteral = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;

        for (int i = 0; i < jsCode.length(); i++) {
            char current = jsCode.charAt(i);
            char next = i + 1 < jsCode.length() ? jsCode.charAt(i + 1) : '\0';

            if (inLineComment) {
                if (current == '\r' || current == '\n') {
                    inLineComment = false;
                    builder.append(current);
                } else {
                    builder.append(' ');
                }
                continue;
            }

            if (inBlockComment) {
                if (current == '*' && next == '/') {
                    builder.append("  ");
                    i++;
                    inBlockComment = false;
                } else {
                    builder.append(current == '\r' || current == '\n' ? current : ' ');
                }
                continue;
            }

            if (inSingleQuote || inDoubleQuote || inTemplateLiteral) {
                builder.append(current);
                if (current == '\\' && i + 1 < jsCode.length()) {
                    builder.append(jsCode.charAt(++i));
                    continue;
                }
                if (inSingleQuote && current == '\'') {
                    inSingleQuote = false;
                } else if (inDoubleQuote && current == '"') {
                    inDoubleQuote = false;
                } else if (inTemplateLiteral && current == '`') {
                    inTemplateLiteral = false;
                }
                continue;
            }

            if (current == '/' && next == '/') {
                builder.append("  ");
                i++;
                inLineComment = true;
            } else if (current == '/' && next == '*') {
                builder.append("  ");
                i++;
                inBlockComment = true;
            } else {
                builder.append(current);
                if (current == '\'') {
                    inSingleQuote = true;
                } else if (current == '"') {
                    inDoubleQuote = true;
                } else if (current == '`') {
                    inTemplateLiteral = true;
                }
            }
        }
        return builder.toString();
    }

    /**
     * 提取第一个 return 语句的返回表达式。
     */
    private static ReturnExpression extractReturnExpression(String jsCode) {
        Pattern returnPattern = Pattern.compile(
                "\\breturn\\b"
        );
        Matcher matcher = returnPattern.matcher(jsCode);
        if (!matcher.find()) {
            return null;
        }
        int expressionStart = skipInlineWhitespace(jsCode, matcher.end());
        if (expressionStart >= jsCode.length()) {
            return null;
        }
        if (jsCode.charAt(expressionStart) == '{') {
            int closeBrace = findMatchingBrace(jsCode, expressionStart);
            if (closeBrace < 0) {
                return null;
            }
            return ReturnExpression.objectLiteral(jsCode.substring(expressionStart + 1, closeBrace));
        }
        String returnedExpression = readSimpleReturnExpression(jsCode, expressionStart).trim();
        return returnedExpression.matches(IDENTIFIER) ? ReturnExpression.variable(returnedExpression) : null;
    }

    private static int skipInlineWhitespace(String jsCode, int start) {
        int index = start;
        while (index < jsCode.length()) {
            char current = jsCode.charAt(index);
            if (current != ' ' && current != '\t' && current != '\f') {
                break;
            }
            index++;
        }
        return index;
    }

    private static String readSimpleReturnExpression(String jsCode, int start) {
        int index = start;
        while (index < jsCode.length()) {
            char current = jsCode.charAt(index);
            if (current == ';' || current == '\r' || current == '\n') {
                break;
            }
            index++;
        }
        return jsCode.substring(start, index);
    }

    private static int findMatchingBrace(String jsCode, int openBraceIndex) {
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inTemplateLiteral = false;

        for (int i = openBraceIndex; i < jsCode.length(); i++) {
            char current = jsCode.charAt(i);
            if (inSingleQuote || inDoubleQuote || inTemplateLiteral) {
                if (current == '\\' && i + 1 < jsCode.length()) {
                    i++;
                    continue;
                }
                if (inSingleQuote && current == '\'') {
                    inSingleQuote = false;
                } else if (inDoubleQuote && current == '"') {
                    inDoubleQuote = false;
                } else if (inTemplateLiteral && current == '`') {
                    inTemplateLiteral = false;
                }
                continue;
            }

            if (current == '\'') {
                inSingleQuote = true;
            } else if (current == '"') {
                inDoubleQuote = true;
            } else if (current == '`') {
                inTemplateLiteral = true;
            } else if (current == '{') {
                depth++;
            } else if (current == '}') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * 提取指定变量赋值为对象字面量的部分（var 或直接赋值）。
     * 返回对象字面量的大括号内容（不含外层大括号）。
     */
    private static String extractObjectLiteral(String jsCode, String varName) {
        // 匹配 var varName = { ... } 或 varName = { ... } (可能带 let/const)
        Pattern pattern = Pattern.compile(
                "(?:\\b(?:var|let|const)\\s+)?\\b" + Pattern.quote(varName) +
                "\\s*=\\s*\\{([^}]*)\\}",
                Pattern.DOTALL
        );
        Matcher matcher = pattern.matcher(jsCode);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    /**
     * 从对象字面量内容中解析 "新字段: record.原字段" 映射。
     */
    private static Map<String, String> parseObjectLiteral(String literalBody) {
        Map<String, String> map = new HashMap<>();
        if (literalBody == null || literalBody.trim().isEmpty()) {
            return map;
        }
        // 按逗号分割属性（简易分割，假设属性值中不含逗号）
        String[] pairs = literalBody.split(",");
        for (String pair : pairs) {
            pair = pair.trim();
            if (pair.isEmpty()) continue;
            // 匹配 newField: record.oldField，字段名可带单引号或双引号
            Pattern pairPattern = Pattern.compile(
                    "(?:['\"])?(" + IDENTIFIER + ")(?:['\"])?\\s*:\\s*\\brecord\\.(" + IDENTIFIER + ")"
            );
            Matcher m = pairPattern.matcher(pair);
            if (m.matches()) {
                String newField = m.group(1);
                String oldField = m.group(2);
                map.put(newField, oldField);
            }
        }
        return map;
    }

    private static final class ReturnExpression {
        private final String variableName;
        private final String objectLiteralBody;

        private ReturnExpression(String variableName, String objectLiteralBody) {
            this.variableName = variableName;
            this.objectLiteralBody = objectLiteralBody;
        }

        private static ReturnExpression variable(String variableName) {
            return new ReturnExpression(variableName, null);
        }

        private static ReturnExpression objectLiteral(String objectLiteralBody) {
            return new ReturnExpression(null, objectLiteralBody);
        }
    }
}
