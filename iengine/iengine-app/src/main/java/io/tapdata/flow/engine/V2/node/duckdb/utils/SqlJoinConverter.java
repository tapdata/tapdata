package io.tapdata.flow.engine.V2.node.duckdb.utils;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;

import java.util.List;

public final class SqlJoinConverter {
    private SqlJoinConverter() {

    }

    public static String convertJoinToInner(String sql, String targetTable) throws Exception {
        Statement statement = CCJSqlParserUtil.parse(sql);
        if (!(statement instanceof Select select)) {
            return sql;
        }
        SelectBody selectBody = select.getSelectBody();
        processSelectBody(selectBody, targetTable);
        return statement.toString();
    }

    private static void processSelectBody(SelectBody selectBody, String targetTable) {
        if (selectBody instanceof PlainSelect plainSelect) {
            List<Join> joins = plainSelect.getJoins();
            if (joins != null) {
                for (Join join : joins) {
                    FromItem rightItem = join.getRightItem();
                    if (rightItem instanceof Table table) {
                        String tableName = table.getName();
                        if (targetTable.equalsIgnoreCase(tableName)) {
                            toInnerJoin(join);
                        }
                    }
                    if (rightItem instanceof SubSelect) {
                        processSelectBody(
                                ((SubSelect) rightItem).getSelectBody(),
                                targetTable
                        );
                    }
                }
            }
            FromItem fromItem = plainSelect.getFromItem();
            if (fromItem instanceof SubSelect) {
                processSelectBody(
                        ((SubSelect) fromItem).getSelectBody(),
                        targetTable
                );
            }
        } else if (selectBody instanceof WithItem withItem) {
            processSelectBody(
                    withItem.getSubSelect().getSelectBody(),
                    targetTable
            );
        } else if (selectBody instanceof SetOperationList set) {
            for (SelectBody body : set.getSelects()) {
                processSelectBody(body, targetTable);
            }
        }
    }

    private static void toInnerJoin(Join join) {
        join.setInner(true);
        join.setLeft(false);
        join.setRight(false);
        join.setFull(false);
        join.setCross(false);
        join.setOuter(false);
    }
}