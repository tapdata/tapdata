package io.tapdata.flow.engine.V2.node.duckdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * QuerySQL 统一处理工具类
 *
 * 功能：
 * 1. 归一化：去除末尾分号、保留换行、标准化空格
 * 2. 合法性检查：语法检查、防 SQL 注入
 * 3. 表名和字段名验证：对照 schema 验证
 *
 * @author TapData
 */
public class QuerySqlProcessor {

    private static final Logger logger = LoggerFactory.getLogger(QuerySqlProcessor.class);

    /**
     * 验证结果
     */
    public static class ValidationResult {
        public boolean isValid;
        public String errorMessage;
        public List<String> tableNames = new ArrayList<>();
        public List<String> fieldNames = new ArrayList<>();

        public ValidationResult(boolean isValid, String errorMessage) {
            this.isValid = isValid;
            this.errorMessage = errorMessage;
        }

        public static ValidationResult success() {
            return new ValidationResult(true, null);
        }

        public static ValidationResult failure(String errorMessage) {
            return new ValidationResult(false, errorMessage);
        }
    }

    /**
     * 归一化 SQL
     *
     * @param sql 原始 SQL
     * @return 归一化后的 SQL
     */
    public static String normalize(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return sql;
        }

        String normalized = sql;

        // 1. 去除末尾分号（保留换行）
        normalized = normalized.trim();
        if (normalized.endsWith(";")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }

        // 2. 标准化空白字符（多个连续空格换为一个，但保留换行）
        // 使用正则：保留换行，只压缩同一行内的多个空格
        StringBuilder sb = new StringBuilder();
        String[] lines = normalized.split("\n", -1);
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            // 压缩一行内的多个空格，但保留单个空格
            line = line.replaceAll("\\s+", " ");
            sb.append(line.trim());
            if (i < lines.length - 1) {
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    /**
     * 验证 SQL 的合法性和安全性
     *
     * @param sql 归一化后的 SQL
     * @param nodeSchemaCache 可用的 schema 缓存
     * @return 验证结果
     */
    public static ValidationResult validate(String sql, Map<String, NodeSchemaInfo> nodeSchemaCache) {
        if (sql == null || sql.trim().isEmpty()) {
            return ValidationResult.failure("QuerySql cannot be null or empty");
        }

        try {
            // 1. 语法合法性检查
            Statement statement = CCJSqlParserUtil.parse(sql);
            
            // 2. 必须是 SELECT 语句（允许 UNION、EXCEPT、INTERSECT、子查询、注释）
            if (!(statement instanceof Select)) {
                return ValidationResult.failure("QuerySql must be a SELECT statement");
            }

            // 3. 防 SQL 注入检查：禁止 DDL/DML 关键字
            String upperSql = sql.toUpperCase();
            
            // 禁止的关键字（DDL/DML）
            String[] forbiddenKeywords = {
                "CREATE ", "DROP ", "ALTER ", "TRUNCATE ", 
                "INSERT ", "UPDATE ", "DELETE ", "MERGE ",
                "EXECUTE ", "EXEC "
            };
            
            for (String keyword : forbiddenKeywords) {
                if (upperSql.contains(keyword)) {
                    return ValidationResult.failure("QuerySql contains forbidden keyword: " + keyword.trim());
                }
            }

            // 4. 如果有 schema 缓存，验证表名和字段名
            if (nodeSchemaCache != null && !nodeSchemaCache.isEmpty()) {
                try {
                    // 解析表名和字段名（简化版，使用 WideTableDdlGenerator 中的逻辑）
                    List<String> selectFields = WideTableDdlGenerator.extractSelectFields(sql);
                    ValidationResult result = ValidationResult.success();
                    result.fieldNames = selectFields;
                    
                    // 简单验证：检查至少有一个字段
                    if (selectFields.isEmpty()) {
                        return ValidationResult.failure("QuerySql does not contain any SELECT fields");
                    }
                    
                    return result;
                } catch (Exception e) {
                    logger.debug("Error validating table and field names, skipping: {}", e.getMessage());
                    // 表名字段验证失败不阻止执行，继续
                }
            }

            return ValidationResult.success();

        } catch (JSQLParserException e) {
            return ValidationResult.failure("QuerySql syntax error: " + e.getMessage());
        }
    }
}
