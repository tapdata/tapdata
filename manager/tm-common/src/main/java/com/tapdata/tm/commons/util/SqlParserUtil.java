package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.dag.process.FromTableConfig;
import com.tapdata.tm.commons.dag.process.duck.JoinField;
import com.tapdata.tm.commons.dag.process.duck.JoinInfo;
import com.tapdata.tm.commons.dag.process.duck.JoinKeyPair;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Slf4j
public class SqlParserUtil {
    private SqlParserUtil() {

    }

    public static List<Field> parseSelectFields(String sql, List<FromTableConfig> fromTables,
                                                  List<Schema> inputSchemas, String mainTableName,
                                                  List<String> wideTablePkColumns,
                                                  List<JoinInfo> joinInfo,
                                                  Map<String, String> aliasTableMap) throws Exception {
        log.info("SqlParserUtil.parseSelectFields() called");
        log.info("  sql: {}", sql);
        log.info("  fromTables: {}", CollectionUtils.isEmpty(fromTables) ? "empty" : fromTables.size());
        log.info("  inputSchemas: {}", CollectionUtils.isEmpty(inputSchemas) ? "empty" : inputSchemas.size());
        log.info("  wideTablePkColumns: {}", wideTablePkColumns);
        
        if (StringUtils.isBlank(sql)) {
            throw new IllegalArgumentException("SQL query cannot be blank");
        }

        // 解析 SQL
        Statement statement;
        try {
            statement = CCJSqlParserUtil.parse(sql);
            log.info("  SQL parsed successfully");
        } catch (JSQLParserException e) {
            log.error("  Failed to parse SQL: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to parse SQL: " + e.getMessage(), e);
        }

        if (!(statement instanceof Select select)) {
            throw new RuntimeException("Only SELECT statements are supported");
        }

        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        buildAliasTableMap(plainSelect, aliasTableMap);
        parseJoinCondition(plainSelect, joinInfo, aliasTableMap);

        // Build alias -> tableName map using visitor pattern
        Map<String, String> aliasToTableNameMap = buildAliasMap(plainSelect);
        log.info("  aliasToTableNameMap: {}", aliasToTableNameMap);

        // 建立表名到 schema 的映射
        Map<String, Schema> tableSchemaMap = buildTableSchemaMap(fromTables, inputSchemas);
        log.info("  tableSchemaMap keys: {}", tableSchemaMap.keySet());

        // 解析选择项
        List<Field> fields = new ArrayList<>();
        Map<String, Boolean> fieldNameMap = new HashMap<>();

        List<SelectItem> selectItems = plainSelect.getSelectItems();
        if (CollectionUtils.isNotEmpty(selectItems)) {
            List<String> pkColumns = new ArrayList<>();
            String fromTableName = null;
            String fromTableAlias = null;
            if (plainSelect.getFromItem() instanceof Table fromTable) {
                fromTableName = fromTable.getName();
                if (fromTable.getAlias() != null) {
                    fromTableAlias = fromTable.getAlias().getName();
                }
            }
            for (String key : wideTablePkColumns) {
                if (StringUtils.isNotBlank(fromTableAlias)) {
                    pkColumns.add(fromTableAlias + "." + key);
                }
                if (StringUtils.isNotBlank(fromTableName)) {
                    pkColumns.add(fromTableName + "." + key);
                }
                pkColumns.add(key);
            }
            for (SelectItem selectItem : selectItems) {
                Field field = parseSelectItem(selectItem, tableSchemaMap, aliasToTableNameMap, wideTablePkColumns, mainTableName);

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

    public static void parseJoinCondition(PlainSelect plainSelect, List<JoinInfo> result, Map<String, String> aliasTableMap) {
        if (plainSelect.getJoins() == null) {
            return;
        }
        for (Join join : plainSelect.getJoins()) {
            JoinInfo info = new JoinInfo();
            if (join.getRightItem() instanceof Table) {
                info.setTable(((Table) join.getRightItem()).getName());
            }
            Expression on = join.getOnExpression();
            parseJoinCondition(on, info.getJoinKeys(), aliasTableMap);
            result.add(info);
        }
    }

    static void parseJoinCondition(
            Expression expr,
            List<JoinKeyPair> joinKeys,
            Map<String, String> aliasTableMap) {
        if (expr == null) {
            return;
        }
        if (expr instanceof AndExpression and) {
            parseJoinCondition(and.getLeftExpression(), joinKeys, aliasTableMap);
            parseJoinCondition(and.getRightExpression(), joinKeys, aliasTableMap);
            return;
        }
        if (expr instanceof EqualsTo equalsTo) {
            if (!(equalsTo.getLeftExpression() instanceof Column leftColumn)
                    || !(equalsTo.getRightExpression() instanceof Column rightColumn)) {
                return;
            }
            JoinKeyPair pair = new JoinKeyPair();
            pair.setLeft(buildField(leftColumn, aliasTableMap));
            pair.setRight(buildField(rightColumn, aliasTableMap));
            joinKeys.add(pair);
        }
    }

    static JoinField buildField(
            Column column,
            Map<String, String> aliasTableMap) {
        JoinField field = new JoinField();
        String alias = column.getTable() == null ? null : column.getTable().getName();
        field.setTable(aliasTableMap.getOrDefault(alias, alias));
        field.setField(column.getColumnName());
        return field;
    }

    private static void buildAliasTableMap(PlainSelect plainSelect, Map<String, String> aliasMap) {
        if (plainSelect.getFromItem() instanceof Table table) {
            String tableName = table.getName();
            if (table.getAlias() != null) {
                aliasMap.put(table.getAlias().getName(), tableName);
            } else {
                aliasMap.put(tableName, tableName);
            }
        }
        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                if (!(join.getRightItem() instanceof Table table)) {
                    continue;
                }
                String tableName = table.getName();
                if (table.getAlias() != null) {
                    aliasMap.put(table.getAlias().getName(), tableName);
                } else {
                    aliasMap.put(tableName, tableName);
                }
            }
        }
    }

    /**
     * Build alias -> real table name map
     * 支持：基础表、表别名、JOIN（LEFT/RIGHT/FULL/INNER）、子查询等
     * 
     * @param plainSelect The parsed SELECT statement
     * @return Map<alias, realTableName>
     */
    public static Map<String, String> buildAliasMap(PlainSelect plainSelect) {
        Map<String, String> aliasToTableNameMap = new HashMap<>();

        try {
            // 使用 TableAliasCollector Visitor 模式，支持复杂场景（子查询、UNION等）
            TableAliasCollector visitor = new TableAliasCollector(aliasToTableNameMap);
            plainSelect.accept(visitor);
        } catch (Exception e) {
            log.warn("Error building alias map: " + e.getMessage());
        }

        return aliasToTableNameMap;
    }

    /**
     * Visitor to collect all table aliases recursively through all SelectBody types
     * Supports: PlainSelect, SetOperationList (UNION, INTERSECT, MINUS), etc.
     * 基于 TablesNamesFinder 风格的实现，简洁且专业
     */
    @Slf4j
    private static class TableAliasCollector implements SelectVisitor {
        private final Map<String, String> aliasMap;

        public TableAliasCollector(Map<String, String> aliasMap) {
            this.aliasMap = aliasMap;
        }

        @Override
        public void visit(PlainSelect plainSelect) {
            try {
                // Process main FROM item
                FromItem fromItem = plainSelect.getFromItem();
                if (fromItem != null) {
                    processFromItem(fromItem);
                }

                // Process JOIN items
                List<Join> joins = plainSelect.getJoins();
                if (CollectionUtils.isNotEmpty(joins)) {
                    for (Join join : joins) {
                        visit(join);
                    }
                }
            } catch (Exception e) {
                log.warn("Error in TableAliasCollector.visit(PlainSelect): " + e.getMessage());
            }
        }

        @Override
        public void visit(SetOperationList setOperationList) {
            try {
                // Process each SelectBody in the SetOperationList
                List<SelectBody> selectBodies = setOperationList.getSelects();
                if (CollectionUtils.isNotEmpty(selectBodies)) {
                    for (SelectBody selectBody : selectBodies) {
                        selectBody.accept(this);
                    }
                }
            } catch (Exception e) {
                log.warn("Error in TableAliasCollector.visit(SetOperationList): " + e.getMessage());
            }
        }

        @Override
        public void visit(WithItem withItem) {
            // WithItem 暂不处理，保持稳定性
        }

        @Override
        public void visit(net.sf.jsqlparser.statement.values.ValuesStatement valuesStatement) {
            // ValuesStatement 暂不处理，保持稳定性
        }

        /**
         * 处理 JOIN 子句
         */
        private void visit(Join join) {
            FromItem rightItem = join.getRightItem();
            if (rightItem != null) {
                processFromItem(rightItem);
            }
        }

        private void processFromItem(FromItem fromItem) {
            if (fromItem instanceof Table table) {
                // 处理普通表
                String realTableName = table.getName();
                if (StringUtils.isNotBlank(realTableName)) {
                    aliasMap.put(realTableName, realTableName);
                    Alias alias = table.getAlias();
                    if (alias != null && StringUtils.isNotBlank(alias.getName())) {
                        aliasMap.put(alias.getName(), realTableName);
                    }
                }
            } else if (fromItem instanceof SubSelect subSelect) {
                // 处理子查询

                // 保存处理前的 aliasMap 副本，用于检测新增的表
                Map<String, String> beforeMap = new HashMap<>(aliasMap);
                
                // 递归处理子查询内容
                SelectBody selectBody = subSelect.getSelectBody();
                if (selectBody != null) {
                    selectBody.accept(this);
                }
                
                // 处理子查询的别名
                Alias subqueryAlias = subSelect.getAlias();
                if (subqueryAlias != null && StringUtils.isNotBlank(subqueryAlias.getName())) {
                    // 查找子查询内部新增的真实表名
                    List<String> newTables = new ArrayList<>();
                    for (Map.Entry<String, String> entry : aliasMap.entrySet()) {
                        // 如果这个 key 在处理前不存在，且是真实表名（key和value相同）
                        if (!beforeMap.containsKey(entry.getKey()) && entry.getKey().equals(entry.getValue())) {
                            newTables.add(entry.getKey());
                        }
                    }
                    
                    // 策略：如果子查询内部有且只有一个真实表，那么将别名映射到该表
                    // 否则映射到自身
                    if (newTables.size() == 1) {
                        aliasMap.put(subqueryAlias.getName(), newTables.get(0));
                    } else {
                        aliasMap.put(subqueryAlias.getName(), subqueryAlias.getName());
                    }
                }
            }
            // 其他 FromItem 类型忽略，保持稳定性
        }
    }

    private static Map<String, Schema> buildTableSchemaMap(List<FromTableConfig> fromTables,
                                                            List<Schema> inputSchemas) {
        log.info("  buildTableSchemaMap() called");
        Map<String, Schema> tableSchemaMap = new HashMap<>();
        Map<String, Schema> inputSchemaMap = new HashMap<>();

        // 将 inputSchemas 转换为 map
        if (CollectionUtils.isNotEmpty(inputSchemas)) {
            log.info("  Processing {} input schemas", inputSchemas.size());
            for (Schema schema : inputSchemas) {
                log.info("    Input schema: nodeId={}, name={}, originalName={}, qualifiedName={}", 
                    schema.getNodeId(), schema.getName(), schema.getOriginalName(), schema.getQualifiedName());
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
            log.info("  Processing {} fromTables", fromTables.size());
            for (FromTableConfig fromTable : fromTables) {
                String tableNameInSql = fromTable.getTableNameInSql();
                String preNodeId = fromTable.getPreNodeId();
                log.info("    FromTableConfig: tableNameInSql={}, preNodeId={}", tableNameInSql, preNodeId);

                // 尝试通过 preNodeId 查找 schema（需要 Node 对象的方法）
                Schema schema = findSchemaByNodeId(inputSchemaMap, preNodeId, tableNameInSql);

                if (schema != null) {
                    log.info("    Found matching schema for fromTable");
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
                } else {
                    log.warn("    No matching schema found for fromTable");
                }
            }
        }

        // 如果没有找到匹配，将所有 inputSchema 都放入
        if (tableSchemaMap.isEmpty() && CollectionUtils.isNotEmpty(inputSchemas)) {
            log.info("  No fromTables match found, falling back to all input schemas");
            for (Schema schema : inputSchemas) {
                String name = StringUtils.isNotBlank(schema.getOriginalName()) ? schema.getOriginalName() : schema.getName();
                if (StringUtils.isNotBlank(name)) {
                    tableSchemaMap.put(name, schema);
                }
            }
        }

        log.info("  buildTableSchemaMap() returning {} entries", tableSchemaMap.size());
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

    private static Field parseSelectItem(SelectItem selectItem, Map<String, Schema> tableSchemaMap, Map<String, String> aliasToTableNameMap,
                                          List<String> wideTablePkColumns, String mainTable) {
        Field field = new Field();
        field.setSource(Field.SOURCE_JOB_ANALYZE);

        if (selectItem instanceof AllColumns) {
            throw new UnsupportedOperationException("SELECT * is not supported, please specify fields explicitly");
        } else if (selectItem instanceof AllTableColumns allTableColumns) {
            throw new UnsupportedOperationException("SELECT table.* is not supported, please specify fields explicitly");
        } else if (selectItem instanceof SelectExpressionItem selectExpressionItem) {
            Expression expression = selectExpressionItem.getExpression();
            Alias alias = selectExpressionItem.getAlias();
            List<Column> referencedColumns = extractColumns(expression);

            String fieldName;
            if (alias != null) {
                fieldName = alias.getName();
            } else if (expression instanceof Column column) {
                fieldName = column.getColumnName();
            } else {
                fieldName = "expr_" + UUID.randomUUID().toString().substring(0, 8);
            }

            field.setFieldName(fieldName);
            if (CollectionUtils.isNotEmpty(referencedColumns)) {
                field.setOriginalFieldName(referencedColumns.get(0).getColumnName());
            } else if (expression instanceof Function function) {
                field.setOriginalFieldName(function.getName());
            } else {
                field.setOriginalFieldName(fieldName);
            }
            if (expression instanceof Column column) {
                Table table = column.getTable();
                String columnName = column.getColumnName();

                if (table != null && StringUtils.isNotBlank(table.getName())) {
                    // Look up real table name using alias map first
                    String tableNameOrAlias = table.getName();
                    String realTableName = aliasToTableNameMap.get(tableNameOrAlias);
                    
                    // Try with real table name first, fall back to original if not found
                    Schema schema = null;
                    if (StringUtils.isNotBlank(realTableName)) {
                        schema = tableSchemaMap.get(realTableName);
                    }
                    if (schema == null) {
                        schema = tableSchemaMap.get(tableNameOrAlias);
                    }
                    boolean allowNullable = !Objects.equals(mainTable, realTableName);
                    copyFieldProperties(schema, columnName, field, wideTablePkColumns, allowNullable);
                } else {
                    for (Schema schema : tableSchemaMap.values()) {
                        copyFieldProperties(schema, columnName, field, wideTablePkColumns, true);
                    }
                }
            } else {
                field.setDataType("VARCHAR");
                field.setJavaType("String");
            }
        }

        return field;
    }

    static void copyFieldProperties(Schema schema, String columnName, Field field, List<String> wideTablePkColumns, boolean allowNullable) {
        if (schema == null) {
            return;
        }
        Field sourceField = findFieldByName(schema, columnName);
        if (sourceField != null) {
            if (!allowNullable) {
                if (sourceField.getIsNullable() instanceof Boolean allow) {
                    allowNullable = allow;
                }
            }
            copyFieldProperties(sourceField, field, wideTablePkColumns, allowNullable);
        }
    }

    private static List<Column> extractColumns(Expression expression) {
        if (expression == null) {
            return Collections.emptyList();
        }
        List<Column> columns = new ArrayList<>();
        expression.accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(Column column) {
                columns.add(column);
            }
        });
        return columns;
    }

    private static void transferPrimaryKeyField(List<Column> referencedColumns, String fieldName,
                                                List<String> wideTablePkColumns, List<String> pkColumns,
                                                Field field) {
        if (CollectionUtils.isEmpty(referencedColumns) || CollectionUtils.isEmpty(wideTablePkColumns)) {
            return;
        }
        LinkedHashSet<String> matchedPrimaryKeys = new LinkedHashSet<>();
        for (Column column : referencedColumns) {
            if (isPrimaryKeyColumn(column, wideTablePkColumns, pkColumns)) {
                matchedPrimaryKeys.add(column.getColumnName());
            }
        }
        Integer firstPrimaryKeyPosition = null;
        for (String primaryKeyColumn : matchedPrimaryKeys) {
            int indexOf = wideTablePkColumns.indexOf(primaryKeyColumn);
            if (indexOf < 0) {
                continue;
            }
            wideTablePkColumns.set(indexOf, fieldName);
            if (firstPrimaryKeyPosition == null) {
                firstPrimaryKeyPosition = indexOf + 1;
            }
        }
    }

    private static boolean isPrimaryKeyColumn(Column column, List<String> wideTablePkColumns, List<String> pkColumns) {
        if (column == null || StringUtils.isBlank(column.getColumnName())) {
            return false;
        }
        String columnName = column.getColumnName();
        if (CollectionUtils.isNotEmpty(wideTablePkColumns) && wideTablePkColumns.contains(columnName)) {
            return true;
        }
        if (CollectionUtils.isEmpty(pkColumns)) {
            return false;
        }
        String fullColumnName = buildFullColumnName(column);
        return pkColumns.contains(columnName) || (StringUtils.isNotBlank(fullColumnName) && pkColumns.contains(fullColumnName));
    }

    private static String buildFullColumnName(Column column) {
        if (column == null || column.getTable() == null || StringUtils.isBlank(column.getTable().getName())) {
            return null;
        }
        return column.getTable().getName() + "." + column.getColumnName();
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

    private static void copyFieldProperties(Field source, Field target, List<String> wideTablePkColumns, boolean allowNullable) {
        target.setDataType(source.getDataType());
        target.setJavaType(source.getJavaType());
        target.setTapType(source.getTapType());
        boolean nullable = wideTablePkColumns != null && wideTablePkColumns.contains(target.getFieldName());
        if (!nullable) {
            nullable = allowNullable;
        }
        target.setIsNullable(nullable);
        target.setPrecision(source.getPrecision());
        target.setScale(source.getScale());
        target.setDefaultValue(source.getDefaultValue());
    }
}
