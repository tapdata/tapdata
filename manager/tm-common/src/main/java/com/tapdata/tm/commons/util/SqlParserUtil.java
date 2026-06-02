package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.dag.process.FromTableConfig;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

@Slf4j
public class SqlParserUtil {

    public static List<Field> parseSelectFields(String sql, List<FromTableConfig> fromTables,
                                                  List<Schema> inputSchemas, Object node) throws Exception {
        if (StringUtils.isBlank(sql)) {
            throw new IllegalArgumentException("SQL query cannot be blank");
        }

        // 解析 SQL
        Statement statement;
        try {
            statement = CCJSqlParserUtil.parse(sql);
        } catch (JSQLParserException e) {
            throw new RuntimeException("Failed to parse SQL: " + e.getMessage(), e);
        }

        if (!(statement instanceof Select)) {
            throw new RuntimeException("Only SELECT statements are supported");
        }

        Select select = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

        // 建立表名到 schema 的映射
        Map<String, Schema> tableSchemaMap = buildTableSchemaMap(fromTables, inputSchemas, node);

        // 解析选择项
        List<Field> fields = new ArrayList<>();
        Map<String, Boolean> fieldNameMap = new HashMap<>();

        List<SelectItem> selectItems = plainSelect.getSelectItems();
        if (CollectionUtils.isNotEmpty(selectItems)) {
            for (SelectItem selectItem : selectItems) {
                Field field = parseSelectItem(selectItem, tableSchemaMap);

                // 检查字段重名
                String fieldName = field.getFieldName();
                if (fieldNameMap.containsKey(fieldName)) {
                    throw new RuntimeException("Duplicate field name: " + fieldName);
                }
                fieldNameMap.put(fieldName, true);
                fields.add(field);
            }
        }

        return fields;
    }

    private static Map<String, Schema> buildTableSchemaMap(List<FromTableConfig> fromTables,
                                                            List<Schema> inputSchemas, Object node) {
        Map<String, Schema> tableSchemaMap = new HashMap<>();
        Map<String, Schema> inputSchemaMap = new HashMap<>();

        // 将 inputSchemas 转换为 map
        if (CollectionUtils.isNotEmpty(inputSchemas)) {
            for (Schema schema : inputSchemas) {
                String qualifiedName = schema.getQualifiedName();
                if (StringUtils.isNotBlank(qualifiedName)) {
                    inputSchemaMap.put(qualifiedName, schema);
                }
                String originalName = schema.getOriginalName();
                if (StringUtils.isNotBlank(originalName)) {
                    inputSchemaMap.put(originalName, schema);
                }
            }
        }

        // 建立 fromTables 到 schema 的映射
        if (CollectionUtils.isNotEmpty(fromTables)) {
            for (FromTableConfig fromTable : fromTables) {
                String tableNameInSql = fromTable.getTableNameInSql();
                String preNodeId = fromTable.getPreNodeId();

                // 尝试通过 preNodeId 查找 schema（需要 Node 对象的方法）
                Schema schema = findSchemaByNodeId(inputSchemaMap, preNodeId, tableNameInSql);

                if (schema != null) {
                    if (StringUtils.isNotBlank(tableNameInSql)) {
                        tableSchemaMap.put(tableNameInSql, schema);
                    }
                    // 同时也添加原始表名
                    String originalName = schema.getOriginalName();
                    if (StringUtils.isNotBlank(originalName)) {
                        tableSchemaMap.put(originalName, schema);
                    }
                    String name = schema.getName();
                    if (StringUtils.isNotBlank(name)) {
                        tableSchemaMap.put(name, schema);
                    }
                }
            }
        }

        // 如果没有找到匹配，将所有 inputSchema 都放入
        if (tableSchemaMap.isEmpty() && CollectionUtils.isNotEmpty(inputSchemas)) {
            for (Schema schema : inputSchemas) {
                String name = StringUtils.isNotBlank(schema.getOriginalName()) ? schema.getOriginalName() : schema.getName();
                if (StringUtils.isNotBlank(name)) {
                    tableSchemaMap.put(name, schema);
                }
            }
        }

        return tableSchemaMap;
    }

    private static Schema findSchemaByNodeId(Map<String, Schema> inputSchemaMap, String preNodeId, String tableName) {
        // 优先通过 tableName 查找
        if (StringUtils.isNotBlank(tableName)) {
            Schema schema = inputSchemaMap.get(tableName);
            if (schema != null) {
                return schema;
            }
        }

        // 尝试其他方式
        if (CollectionUtils.isNotEmpty(inputSchemaMap.values())) {
            return inputSchemaMap.values().iterator().next();
        }

        return null;
    }

    private static Field parseSelectItem(SelectItem selectItem, Map<String, Schema> tableSchemaMap) {
        Field field = new Field();
        field.setSource(Field.SOURCE_JOB_ANALYZE);

        if (selectItem instanceof AllColumns) {
            // SELECT * FROM ... - 返回所有字段
            throw new UnsupportedOperationException("SELECT * is not supported, please specify fields explicitly");
        } else if (selectItem instanceof AllTableColumns allTableColumns) {
            // SELECT table.* FROM ... - 返回指定表的所有字段
            Table table = allTableColumns.getTable();
            throw new UnsupportedOperationException("SELECT table.* is not supported, please specify fields explicitly");
        } else if (selectItem instanceof SelectExpressionItem selectExpressionItem) {
            Expression expression = selectExpressionItem.getExpression();
            Alias alias = selectExpressionItem.getAlias();

            // 设置字段名
            String fieldName;
            if (alias != null) {
                fieldName = alias.getName();
            } else if (expression instanceof Column column) {
                fieldName = column.getColumnName();
            } else {
                // 表达式没有别名，需要生成一个
                fieldName = "expr_" + UUID.randomUUID().toString().substring(0, 8);
            }

            field.setFieldName(fieldName);
            field.setOriginalFieldName(fieldName);

            // 尝试从源表获取字段信息
            if (expression instanceof Column column) {
                Table table = column.getTable();
                String columnName = column.getColumnName();

                if (table != null && StringUtils.isNotBlank(table.getName())) {
                    // 有表名，尝试查找对应的 schema
                    Schema schema = tableSchemaMap.get(table.getName());
                    if (schema != null) {
                        Field sourceField = findFieldByName(schema, columnName);
                        if (sourceField != null) {
                            // 复制源字段的属性
                            copyFieldProperties(sourceField, field);
                        }
                    }
                } else {
                    // 没有表名，遍历所有 schema 查找
                    for (Schema schema : tableSchemaMap.values()) {
                        Field sourceField = findFieldByName(schema, columnName);
                        if (sourceField != null) {
                            copyFieldProperties(sourceField, field);
                            break;
                        }
                    }
                }
            } else {
                // 表达式字段，设置默认类型
                field.setDataType("VARCHAR");
                field.setJavaType("String");
            }
        }

        return field;
    }

    private static Field findFieldByName(Schema schema, String fieldName) {
        if (schema == null || schema.getFields() == null) {
            return null;
        }
        for (Field field : schema.getFields()) {
            if (fieldName.equals(field.getFieldName())) {
                return field;
            }
        }
        return null;
    }

    private static void copyFieldProperties(Field source, Field target) {
        target.setDataType(source.getDataType());
        target.setJavaType(source.getJavaType());
        target.setTapType(source.getTapType());
        target.setPrimaryKey(source.getPrimaryKey());
        target.setPrimaryKeyPosition(source.getPrimaryKeyPosition());
        target.setIsNullable(source.getIsNullable());
        target.setPrecision(source.getPrecision());
        target.setScale(source.getScale());
        target.setDefaultValue(source.getDefaultValue());
    }
}
