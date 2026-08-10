package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapField;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 宽表 DDL 生成器（优化版）
 * 
 * <p>核心功能：根据 querySql 解析 SELECT 字段，自动生成 CREATE TABLE DDL 语句</p>
 * 
 * <p>支持场景：</p>
 * <ul>
 *   <li>基础字段：id, name, age</li>
 *   <li>带别名字段：id as user_id, name as user_name</li>
 *   <li>表达式字段：COUNT(*) as cnt, SUM(amount) as total</li>
 * </ul>
 */
public class WideTableDdlGenerator {

    private static final Logger logger = LoggerFactory.getLogger(WideTableDdlGenerator.class);
    
    /** DuckDB 保留字集合（用于标识符转义） */
    private static final Set<String> RESERVED_WORDS = new HashSet<>(Arrays.asList(
        "select", "from", "where", "and", "or", "not", "in", "is", "null",
        "true", "false", "order", "by", "group", "having", "limit", "offset",
        "join", "inner", "left", "right", "full", "outer", "cross", "on",
        "as", "distinct", "all", "union", "intersect", "except", "with"
    ));
    private static final Pattern DECIMAL_TYPE_PATTERN =
            Pattern.compile("^(DECIMAL|NUMERIC)(?:\\s*\\(\\s*(\\d+)\\s*(?:,\\s*(\\d+)\\s*)?\\)|\\b).*");

    /**
     * 字段信息封装类
     */
    public static class FieldInfo {
        private final String fieldName;
        private final String fieldType;
        private final boolean isExpression;
        
        public FieldInfo(String fieldName, String fieldType, boolean isExpression) {
            this.fieldName = fieldName;
            this.fieldType = fieldType;
            this.isExpression = isExpression;
        }
        
        public String getFieldName() { return fieldName; }
        public String getFieldType() { return fieldType; }
        public boolean isExpression() { return isExpression; }
    }

    /**
     * 从 querySql 中提取 SELECT 字段列表（返回字段名）
     */
    public static List<String> extractSelectFields(String querySql) {
        List<FieldInfo> fieldInfos = extractSelectFieldsWithType(querySql);
        return fieldInfos.stream()
                .map(FieldInfo::getFieldName)
                .collect(Collectors.toList());
    }

    /**
     * 从 querySql 中提取 SELECT 字段列表（返回字段名和类型）
     */
    public static List<FieldInfo> extractSelectFieldsWithType(String querySql) {
        List<FieldInfo> fields = new ArrayList<>();
        
        if (querySql == null || querySql.isBlank()) {
            logger.warn("querySql is null or blank");
            return fields;
        }

        try {
            // 方法1：使用 JSqlParser 解析（优先）
            fields = parseWithJsqlParser(querySql);
            if (!fields.isEmpty()) {
                logger.info("Successfully parsed {} fields from querySql using JSqlParser", fields.size());
                return fields;
            }
        } catch (Exception e) {
            logger.warn("JSqlParser parsing failed: {}", e.getMessage());
        }

        // 方法2：使用正则表达式提取（降级方案）
        try {
            fields = parseWithRegex(querySql);
            if (!fields.isEmpty()) {
                logger.info("Successfully parsed {} fields from querySql using regex", fields.size());
                return fields;
            }
        } catch (Exception e) {
            logger.error("Regex parsing failed: {}", e.getMessage(), e);
        }

        return fields;
    }

    /**
     * 使用 JSqlParser 解析字段
     */
    private static List<FieldInfo> parseWithJsqlParser(String querySql) throws JSQLParserException {
        List<FieldInfo> fields = new ArrayList<>();
        
        Statement statement = CCJSqlParserUtil.parse(querySql);
        
        if (statement instanceof Select select) {
            SelectBody selectBody = select.getSelectBody();
            
            if (selectBody instanceof PlainSelect) {
                PlainSelect plainSelect = (PlainSelect) selectBody;
                List<SelectItem> selectItems = plainSelect.getSelectItems();
                
                if (selectItems != null) {
                    for (SelectItem selectItem : selectItems) {
                        FieldInfo fieldInfo = extractFieldInfo(selectItem);
                        if (fieldInfo != null && fieldInfo.getFieldName() != null && !fieldInfo.getFieldName().isBlank()) {
                            fields.add(fieldInfo);
                        }
                    }
                }
            } else if (selectBody instanceof SetOperationList) {
                // 处理 UNION/UNION ALL - 只从第一个 SELECT 提取字段
                SetOperationList setOperationList = (SetOperationList) selectBody;
                List<SelectBody> selectBodies = setOperationList.getSelects();
                if (selectBodies != null && !selectBodies.isEmpty()) {
                    SelectBody firstBody = selectBodies.get(0);
                    if (firstBody instanceof PlainSelect) {
                        PlainSelect plainSelect = (PlainSelect) firstBody;
                        List<SelectItem> selectItems = plainSelect.getSelectItems();
                        if (selectItems != null) {
                            for (SelectItem selectItem : selectItems) {
                                FieldInfo fieldInfo = extractFieldInfo(selectItem);
                                if (fieldInfo != null && fieldInfo.getFieldName() != null && !fieldInfo.getFieldName().isBlank()) {
                                    fields.add(fieldInfo);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return fields;
    }

    /**
     * 从 SelectItem 中提取字段信息
     */
    private static FieldInfo extractFieldInfo(SelectItem selectItem) {
        if (selectItem == null) {
            return null;
        }

        String fieldName = null;
        String fieldType = "VARCHAR";
        boolean isExpression = false;

        // 处理 SelectExpressionItem
        if (selectItem instanceof SelectExpressionItem) {
            SelectExpressionItem expressionItem = (SelectExpressionItem) selectItem;
            
            // 获取别名
            Alias alias = expressionItem.getAlias();
            if (alias != null && alias.getName() != null) {
                fieldName = alias.getName();
            }
            
            Expression expression = expressionItem.getExpression();
            
            // 如果没有别名，尝试从表达式提取
            if (fieldName == null) {
                if (expression instanceof Column) {
                    fieldName = ((Column) expression).getColumnName();
                } else if (expression instanceof Function) {
                    Function func = (Function) expression;
                    fieldName = func.getName();
                }
            }
            
            // 判断是否为表达式
            isExpression = !(expression instanceof Column);
        } else if (selectItem instanceof AllColumns) {
            return new FieldInfo("*", "VARCHAR", false);
        }

        // 如果仍然没有字段名，生成一个唯一名称
        if (fieldName == null || fieldName.isBlank()) {
            fieldName = "expr_" + System.currentTimeMillis() + "_" + 
                       String.format("%04d", (int) (Math.random() * 10000));
            isExpression = true;
        }

        return new FieldInfo(fieldName, fieldType, isExpression);
    }

    /**
     * 使用正则表达式提取字段
     */
    private static List<FieldInfo> parseWithRegex(String querySql) {
        List<FieldInfo> fields = new ArrayList<>();
        Pattern selectPattern = Pattern.compile("(?is)^\\s*SELECT\\s+(.*?)(?=\\s+FROM\\b)");
        Matcher selectMatcher = selectPattern.matcher(querySql);
        
        if (selectMatcher.find()) {
            String selectClause = selectMatcher.group(1).trim();
            String[] parts = splitFields(selectClause);
            
            for (String part : parts) {
                String fieldName = extractFieldNameFromPart(part.trim());
                if (fieldName != null && !fieldName.isBlank()) {
                    boolean isExpression = part.contains("(") && part.contains(")");
                    fields.add(new FieldInfo(fieldName, "VARCHAR", isExpression));
                }
            }
        }
        
        return fields;
    }

    /**
     * 分割字段列表（考虑括号内的逗号）
     */
    private static String[] splitFields(String selectClause) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int parenDepth = 0;
        
        for (char c : selectClause.toCharArray()) {
            if (c == '(') {
                parenDepth++;
                current.append(c);
            } else if (c == ')') {
                parenDepth--;
                current.append(c);
            } else if (c == ',' && parenDepth == 0) {
                fields.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        
        if (current.length() > 0) {
            fields.add(current.toString());
        }
        
        return fields.toArray(new String[0]);
    }

    /**
     * 从字段部分提取字段名
     */
    private static String extractFieldNameFromPart(String part) {
        part = part.trim();
        
        // 处理 AS 别名
        final Pattern asPattern = Pattern.compile("(?i)\\s+AS\\s+([A-Za-z_][A-Za-z0-9_]*)\\s*$");
        Matcher asMatcher = asPattern.matcher(part);
        if (asMatcher.find()) {
            return asMatcher.group(1).trim();
        }
        
        // 处理不带 AS 的别名
        String[] tokens = part.split("[\\s,]+");
        if (tokens.length >= 2) {
            String lastToken = tokens[tokens.length - 1].trim();
            if (!lastToken.isEmpty() && !lastToken.equals("*") && 
                !RESERVED_WORDS.contains(lastToken.toLowerCase()) &&
                !lastToken.contains("(")) {
                return lastToken;
            }
        }
        
        // 处理简单字段名
        if (tokens.length == 1 && !tokens[0].equals("*")) {
            return tokens[0];
        }
        
        // 处理函数调用
        Pattern funcPattern = Pattern.compile("^\\s*(\\w+)\\s*\\(");
        Matcher funcMatcher = funcPattern.matcher(part);
        if (funcMatcher.find()) {
            return funcMatcher.group(1);
        }
        
        return null;
    }

    /**
     * 使用 CREATE TABLE ... AS SELECT 语法生成建表语句（推荐）
     */
    public static String generateCreateTableAsSelect(String tableName, String querySql) {
        validateTableName(tableName);
        validateQuerySql(querySql);

        String trimmedSql = querySql.trim();
        
        // 使用子查询 alias 方式构建建表语句
        String ddl = String.format("CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM (%s) AS t WHERE 1=0", 
                                   quoteIdentifier(tableName), trimmedSql);
        
        logger.info("Generated CREATE TABLE AS SELECT DDL with subquery alias: {}", ddl);
        return ddl;
    }

    /**
     * 根据字段列表生成 CREATE TABLE DDL（传统方式）
     */
    public static String generateCreateTableDdl(String tableName, List<String> fields, List<String> primaryKey) {
        validateTableName(tableName);
        validateFields(fields);

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS ").append(quoteIdentifier(tableName)).append(" (\n");

        List<String> columnDefs = new ArrayList<>();
        for (String field : fields) {
            columnDefs.add("    " + quoteIdentifier(field) + " VARCHAR");
        }

        StringJoiner pkQuote = new StringJoiner(",");
        primaryKey.forEach(pk -> {
            if (pk != null && !pk.isBlank() && fields.contains(pk)) {
                pkQuote.add(quoteIdentifier(pk));
            }
        });
        if (pkQuote.length() > 0) {
            columnDefs.add("    PRIMARY KEY (" + quoteIdentifier(pkQuote.toString()) + ")");
        }

        ddl.append(String.join(",\n", columnDefs));
        ddl.append("\n)");

        logger.info("Generated CREATE TABLE DDL: {}", ddl);
        return ddl.toString();
    }

    /**
     * 生成带完整字段类型的 CREATE TABLE DDL
     */
    public static String generateCreateTableDdl(String tableName, Map<String, String> fieldTypeMap, String primaryKey) {
        validateTableName(tableName);
        if (fieldTypeMap == null || fieldTypeMap.isEmpty()) {
            throw new IllegalArgumentException("fieldTypeMap cannot be null or empty");
        }

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS ").append(quoteIdentifier(tableName)).append(" (\n");

        List<String> columnDefs = new ArrayList<>();
        for (Map.Entry<String, String> entry : fieldTypeMap.entrySet()) {
            String fieldName = entry.getKey();
            String fieldType = entry.getValue() != null ? entry.getValue() : "VARCHAR";
            columnDefs.add("    " + quoteIdentifier(fieldName) + " " + fieldType);
        }

        if (primaryKey != null && !primaryKey.isBlank() && fieldTypeMap.containsKey(primaryKey)) {
            columnDefs.add("    PRIMARY KEY (" + quoteIdentifier(primaryKey) + ")");
        }

        ddl.append(String.join(",\n", columnDefs));
        ddl.append("\n)");

        logger.info("Generated CREATE TABLE DDL with field types: {}", ddl);
        return ddl.toString();
    }

    /**
     * 根据字段信息列表生成 CREATE TABLE DDL
     */
    public static String generateCreateTableDdlWithFieldInfo(String tableName, List<FieldInfo> fieldInfos, String primaryKey) {
        validateTableName(tableName);
        if (fieldInfos == null || fieldInfos.isEmpty()) {
            throw new IllegalArgumentException("fieldInfos cannot be null or empty");
        }

        Map<String, String> fieldTypeMap = fieldInfos.stream()
                .collect(Collectors.toMap(
                    FieldInfo::getFieldName,
                    FieldInfo::getFieldType,
                    (existing, replacement) -> existing
                ));

        return generateCreateTableDdl(tableName, fieldTypeMap, primaryKey);
    }

    /**
     * 转义标识符（处理保留字和特殊字符）
     */
    public static String quoteIdentifier(String identifier) {
        if (identifier == null) {
            return "\"null\"";
        }
        if (needsQuoting(identifier)) {
            return "\"" + identifier.replace("\"", "\"\"") + "\"";
        }
        return identifier;
    }

    /**
     * 判断标识符是否需要转义
     */
    private static boolean needsQuoting(String identifier) {
        if (RESERVED_WORDS.contains(identifier.toLowerCase())) {
            return true;
        }
        return !identifier.matches("^[a-zA-Z_][a-zA-Z0-9_]*$");
    }

    /**
     * 推断 DuckDB 字段类型（基于 Java 类型）
     */
    public static String inferDuckDbType(Class<?> javaType) {
        if (javaType == null) {
            return "VARCHAR";
        }
        
        if (javaType == String.class || javaType == CharSequence.class) {
            return "VARCHAR";
        } else if (javaType == Integer.class || javaType == Long.class ||
                   javaType == int.class || javaType == long.class) {
            return "BIGINT";
        } else if (javaType == Double.class || javaType == Float.class ||
                   javaType == double.class || javaType == float.class) {
            return "DOUBLE";
        } else if (javaType == Boolean.class || javaType == boolean.class) {
            return "BOOLEAN";
        } else if (javaType == java.sql.Date.class || javaType == java.util.Date.class) {
            return "DATE";
        } else if (javaType == java.sql.Timestamp.class) {
            return "TIMESTAMP";
        } else if (javaType == byte[].class) {
            return "BLOB";
        } else {
            return "VARCHAR";
        }
    }

    private static void validateTableName(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName cannot be null or blank");
        }
    }

    private static void validateQuerySql(String querySql) {
        if (querySql == null || querySql.isBlank()) {
            throw new IllegalArgumentException("querySql cannot be null or blank");
        }
        if (!querySql.trim().toLowerCase().startsWith("select")) {
            throw new IllegalArgumentException("querySql must be a SELECT statement");
        }
    }

    private static void validateFields(List<String> fields) {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("fields cannot be null or empty");
        }
    }
    
    /**
     * 根据宽表 NodeSchemaInfo 生成完整的 CREATE TABLE DDL
     * 直接使用预计算的类型信息，避免重复转换
     */
    public static String generateCreateTableDdl(NodeSchemaInfo wideTableSchemaInfo) {
        if (wideTableSchemaInfo == null) {
            throw new IllegalArgumentException("wideTableSchemaInfo cannot be null");
        }
        
        String tableName = wideTableSchemaInfo.getTableName();
        Map<String, TapField> fieldMap = wideTableSchemaInfo.getFieldMap();

        if (fieldMap == null || fieldMap.isEmpty()) {
            throw new IllegalArgumentException("fieldMap in wideTableSchemaInfo cannot be null or empty");
        }

        validateTableName(tableName);
        
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS ").append(quoteIdentifier(tableName)).append(" (\n");
        
        List<String> columnDefs = new ArrayList<>();
        for (TapField tapField : wideTableSchemaInfo.getOrderedFields()) {
            String fieldName = tapField.getName();
            String duckDbType = convertTapTypeToDuckDbType(tapField);
            columnDefs.add("    " + quoteIdentifier(fieldName) + " " + duckDbType);
        }
        ddl.append(String.join(",\n", columnDefs));
        ddl.append("\n)");
        
        logger.info("Generated CREATE TABLE DDL from NodeSchemaInfo: {}", ddl);
        return ddl.toString();
    }

    public static String generateIndex(NodeSchemaInfo wideTableSchemaInfo, List<String> primaryKey) {
        Map<String, TapField> fieldMap = wideTableSchemaInfo.getFieldMap();
        if (fieldMap == null || fieldMap.isEmpty()) {
            throw new IllegalArgumentException("fieldMap in wideTableSchemaInfo cannot be null or empty");
        }
        String tableName = wideTableSchemaInfo.getTableName();
        StringJoiner pkQuote = new StringJoiner(",");
        StringJoiner indexName = new StringJoiner("_");
        indexName.add(tableName);
        primaryKey.forEach(pk -> {
            if (pk != null && !pk.isBlank() && fieldMap.containsKey(pk)) {
                pkQuote.add(quoteIdentifier(pk));
                indexName.add(pk);
            }
        });
        if (pkQuote.length() > 0) {
            return String.format("CREATE UNIQUE INDEX %s ON %s (%s)", indexName, tableName, pkQuote);
        }
        return null;
    }

    
    /**
     * 将 TapField 转换为 DuckDB 类型字符串
     */
    private static String convertTapTypeToDuckDbType(TapField tapField) {
        if (tapField == null) {
            return "VARCHAR";
        }
        
        // 优先从 TapFieldDto 的预计算类型获取（如果有）
        // 这里我们直接从 TapField 推断 DuckDB 类型
        String dataType = tapField.getDataType();
        if (dataType != null) {
            return mapDataTypeToDuckDbType(dataType);
        }
        
        return "VARCHAR";
    }
    
    /**
     * 根据数据类型字符串映射到 DuckDB 类型
     */
    private static String mapDataTypeToDuckDbType(String dataType) {
        if (dataType == null) {
            return "VARCHAR";
        }
        String upperType = dataType.trim().toUpperCase(Locale.ROOT);
        Matcher decimalMatcher = DECIMAL_TYPE_PATTERN.matcher(upperType);
        if (decimalMatcher.matches()) {
            String precision = decimalMatcher.group(2);
            String scale = decimalMatcher.group(3);
            if (precision != null && scale != null) {
                return "DECIMAL(" + precision + "," + scale + ")";
            }
            if (precision != null) {
                return "DECIMAL(" + precision + ")";
            }
            return "DECIMAL";
        }

        int paren = upperType.indexOf('(');
        if (paren > 0) {
            upperType = upperType.substring(0, paren).trim();
        }
        int space = upperType.indexOf(' ');
        if (space > 0) {
            upperType = upperType.substring(0, space).trim();
        }

        switch (upperType) {
            case "STRING":
            case "TEXT":
            case "VARCHAR":
            case "CHAR":
                return "VARCHAR";
            case "INT":
            case "INTEGER":
            case "LONG":
            case "BIGINT":
            case "TINYINT":
            case "SMALLINT":
                return "BIGINT";
            case "FLOAT":
            case "DOUBLE":
            case "NUMBER":
                return "DOUBLE";
            case "BOOLEAN":
            case "BOOL":
                return "BOOLEAN";
            case "DATE":
                return "DATE";
            case "TIME":
                return "TIME";
            case "DATETIME":
            case "TIMESTAMP":
                return "TIMESTAMP";
            case "BINARY":
            case "BLOB":
                return "BLOB";
            default:
                return "VARCHAR";
        }
    }
}
