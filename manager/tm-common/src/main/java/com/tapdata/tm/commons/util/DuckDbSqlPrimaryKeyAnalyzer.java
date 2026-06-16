package com.tapdata.tm.commons.util;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public final class DuckDbSqlPrimaryKeyAnalyzer {
    private DuckDbSqlPrimaryKeyAnalyzer() {
    }

    public static List<String> analyzePrimaryKeys(String sql) {
        if (StringUtils.isBlank(sql)) {
            return Collections.emptyList();
        }

        PlainSelect plainSelect = parsePlainSelect(sql);
        if (plainSelect == null) {
            return Collections.emptyList();
        }

        Map<String, String> aliasToTableMap = SqlParserUtil.buildAliasMap(plainSelect);
        if (plainSelect.getJoins() == null || plainSelect.getJoins().isEmpty()) {
            return Collections.emptyList();
        }

        List<SelectProjection> selectProjections = buildSelectProjections(plainSelect.getSelectItems(), aliasToTableMap);
        if (selectProjections.isEmpty()) {
            return Collections.emptyList();
        }

        LinkedHashSet<String> joinKeyFieldNames = new LinkedHashSet<>();
        LinkedHashSet<String> seenJoinColumnKeys = new LinkedHashSet<>();
        for (net.sf.jsqlparser.statement.select.Join join : plainSelect.getJoins()) {
            List<JoinKeyPair> joinKeyPairs = extractJoinKeyPairs(join.getOnExpression(), aliasToTableMap);
            for (JoinKeyPair pair : joinKeyPairs) {
                if (seenJoinColumnKeys.contains(pair.getLeftColumnKey())
                        || seenJoinColumnKeys.contains(pair.getRightColumnKey())) {
                    continue;
                }
                String fieldName = findSelectFieldName(selectProjections, pair.getLeftColumnKey());
                if (StringUtils.isBlank(fieldName)) {
                    fieldName = findSelectFieldName(selectProjections, pair.getRightColumnKey());
                }
                seenJoinColumnKeys.add(pair.getLeftColumnKey());
                seenJoinColumnKeys.add(pair.getRightColumnKey());
                if (StringUtils.isNotBlank(fieldName)) {
                    joinKeyFieldNames.add(fieldName);
                }
            }
        }

        return new ArrayList<>(joinKeyFieldNames);
    }

    private static PlainSelect parsePlainSelect(String sql) {
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            if (!(statement instanceof Select select)) {
                return null;
            }
            if (!(select.getSelectBody() instanceof PlainSelect plainSelect)) {
                return null;
            }
            return plainSelect;
        } catch (JSQLParserException e) {
            log.warn("Failed to analyze primary keys from sql, sql={}", sql, e);
            return null;
        }
    }

    private static List<SelectProjection> buildSelectProjections(List<SelectItem> selectItems, Map<String, String> aliasToTableMap) {
        if (selectItems == null || selectItems.isEmpty()) {
            return Collections.emptyList();
        }
        List<SelectProjection> projections = new ArrayList<>();
        for (SelectItem selectItem : selectItems) {
            if (!(selectItem instanceof SelectExpressionItem selectExpressionItem)) {
                continue;
            }
            String outputFieldName = resolveSelectOutputFieldName(selectExpressionItem);
            if (StringUtils.isBlank(outputFieldName)) {
                continue;
            }
            Set<String> referencedColumns = extractNormalizedColumns(selectExpressionItem.getExpression(), aliasToTableMap);
            if (referencedColumns.isEmpty()) {
                continue;
            }
            projections.add(new SelectProjection(outputFieldName, referencedColumns));
        }
        return projections;
    }

    private static String resolveSelectOutputFieldName(SelectExpressionItem selectExpressionItem) {
        Alias alias = selectExpressionItem.getAlias();
        if (alias != null && StringUtils.isNotBlank(alias.getName())) {
            return alias.getName();
        }
        Expression expression = selectExpressionItem.getExpression();
        if (expression instanceof Column column) {
            return column.getColumnName();
        }
        return null;
    }

    private static String findSelectFieldName(List<SelectProjection> projections, String columnKey) {
        if (projections == null || projections.isEmpty() || StringUtils.isBlank(columnKey)) {
            return null;
        }
        for (SelectProjection projection : projections) {
            if (projection.getReferencedColumnKeys().contains(columnKey)) {
                return projection.getOutputFieldName();
            }
        }
        return null;
    }

    private static List<JoinKeyPair> extractJoinKeyPairs(Expression expression, Map<String, String> aliasToTableMap) {
        if (expression == null) {
            return Collections.emptyList();
        }
        List<JoinKeyPair> pairs = new ArrayList<>();
        collectJoinKeyPairs(expression, aliasToTableMap, pairs);
        return pairs;
    }

    private static void collectJoinKeyPairs(Expression expression, Map<String, String> aliasToTableMap, List<JoinKeyPair> pairs) {
        if (expression == null) {
            return;
        }
        if (expression instanceof Parenthesis parenthesis) {
            collectJoinKeyPairs(parenthesis.getExpression(), aliasToTableMap, pairs);
            return;
        }
        if (expression instanceof AndExpression andExpression) {
            collectJoinKeyPairs(andExpression.getLeftExpression(), aliasToTableMap, pairs);
            collectJoinKeyPairs(andExpression.getRightExpression(), aliasToTableMap, pairs);
            return;
        }
        if (!(expression instanceof EqualsTo equalsTo)) {
            return;
        }
        if (!(equalsTo.getLeftExpression() instanceof Column leftColumn)
                || !(equalsTo.getRightExpression() instanceof Column rightColumn)) {
            return;
        }
        String leftKey = normalizeColumn(leftColumn, aliasToTableMap);
        String rightKey = normalizeColumn(rightColumn, aliasToTableMap);
        if (StringUtils.isAnyBlank(leftKey, rightKey)) {
            return;
        }
        pairs.add(new JoinKeyPair(leftKey, rightKey));
    }

    private static Set<String> extractNormalizedColumns(Expression expression, Map<String, String> aliasToTableMap) {
        if (expression == null) {
            return Collections.emptySet();
        }
        Set<String> columns = new LinkedHashSet<>();
        expression.accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(Column column) {
                String normalized = normalizeColumn(column, aliasToTableMap);
                if (StringUtils.isNotBlank(normalized)) {
                    columns.add(normalized);
                }
            }
        });
        return columns;
    }

    private static String normalizeColumn(Column column, Map<String, String> aliasToTableMap) {
        if (column == null || StringUtils.isBlank(column.getColumnName())) {
            return null;
        }
        String tableName = null;
        if (column.getTable() != null) {
            tableName = column.getTable().getName();
        }
        tableName = normalizeTableName(tableName, aliasToTableMap);
        if (StringUtils.isBlank(tableName)) {
            return null;
        }
        return buildColumnKey(tableName, column.getColumnName());
    }

    private static String normalizeTableName(String tableName, Map<String, String> aliasToTableMap) {
        if (StringUtils.isBlank(tableName)) {
            return null;
        }
        String normalized = aliasToTableMap.getOrDefault(tableName, tableName);
        return normalized == null ? null : normalized.trim().toLowerCase();
    }

    private static String buildColumnKey(String tableName, String columnName) {
        if (StringUtils.isAnyBlank(tableName, columnName)) {
            return null;
        }
        return tableName.trim().toLowerCase() + "." + columnName.trim().toLowerCase();
    }

    private static final class SelectProjection {
        private final String outputFieldName;
        private final Set<String> referencedColumnKeys;

        private SelectProjection(String outputFieldName, Set<String> referencedColumnKeys) {
            this.outputFieldName = outputFieldName;
            this.referencedColumnKeys = referencedColumnKeys;
        }

        private String getOutputFieldName() {
            return outputFieldName;
        }

        private Set<String> getReferencedColumnKeys() {
            return referencedColumnKeys;
        }
    }

    private static final class JoinKeyPair {
        private final String leftColumnKey;
        private final String rightColumnKey;

        private JoinKeyPair(String leftColumnKey, String rightColumnKey) {
            this.leftColumnKey = leftColumnKey;
            this.rightColumnKey = rightColumnKey;
        }

        private String getLeftColumnKey() {
            return leftColumnKey;
        }

        private String getRightColumnKey() {
            return rightColumnKey;
        }
    }
}
