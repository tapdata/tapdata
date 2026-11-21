package com.tapdata.tm.commons.inspect;
import org.apache.commons.collections4.CollectionUtils;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.XMLSerializeExpr;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.util.TablesNamesFinder;
import java.util.List;
public final class CustomQueryReadOnly {
    public static void validateReadOnlyQuery(String sql, String connectionName) {
        try {
            Statements statements = CCJSqlParserUtil.parseStatements(sql);
            if (statements == null || statements.getStatements() == null || statements.getStatements().size() != 1) {
                throw new IllegalArgumentException("Exactly one SQL statement is required");
            }
            Statement statement = statements.getStatements().get(0);
            if (!(statement instanceof Select)) {
                throw new IllegalArgumentException("Only read-only SELECT queries are allowed");
            }
            Select select = (Select) statement;
            if (select.getSelectBody() == null) {
                throw new IllegalArgumentException("SQL query did not contain a SELECT body");
            }
            // Walks every nested SELECT: FROM/JOIN subqueries, expression subqueries, set operations and CTEs.
            new ReadOnlySelectValidator().getTableList((Statement) select);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid or non-read-only SQL query on " + connectionName + ": " + e.getMessage(), e);
        }
    }

    private static class ReadOnlySelectValidator extends TablesNamesFinder {
        @Override
        public void visit(PlainSelect plainSelect) {
            if (CollectionUtils.isNotEmpty(plainSelect.getIntoTables()) || plainSelect.isForUpdate()) {
                throw new IllegalArgumentException("SELECT INTO and FOR UPDATE are not allowed");
            }
            super.visit(plainSelect);
            // TablesNamesFinder 4.3 skips these expressions, including their nested SELECTs.
            visitJoinConditions(plainSelect.getJoins());
            visitOrderBy(plainSelect.getOrderByElements());
            if (plainSelect.getGroupBy() != null && plainSelect.getGroupBy().getGroupByExpressions() != null) {
                plainSelect.getGroupBy().getGroupByExpressions().forEach(expression -> expression.accept(this));
            }
        }

        @Override
        public void visit(SetOperationList setOperations) {
            super.visit(setOperations);
            visitOrderBy(setOperations.getOrderByElements());
        }

        @Override
        public void visit(SubJoin subJoin) {
            super.visit(subJoin);
            visitJoinConditions(subJoin.getJoinList());
        }

        private void visitJoinConditions(List<Join> joins) {
            if (joins != null) {
                joins.forEach(join -> {
                    if (join.getOnExpressions() != null) {
                        join.getOnExpressions().forEach(expression -> expression.accept(this));
                    }
                });
            }
        }

        private void visitOrderBy(List<OrderByElement> orderBy) {
            if (orderBy != null) {
                orderBy.forEach(order -> order.getExpression().accept(this));
            }
        }

        // TablesNamesFinder rejects the following read-only constructs; keep walking their arguments instead.
        @Override
        public void visit(TableFunction tableFunction) {
            if (tableFunction.getFunction() != null) {
                tableFunction.getFunction().accept(this);
            }
        }

        @Override
        public void visit(ArrayConstructor arrayConstructor) {
            if (arrayConstructor.getExpressions() != null) {
                arrayConstructor.getExpressions().forEach(expression -> expression.accept(this));
            }
        }

        @Override
        public void visit(OracleHint hint) {
            // Hints do not contain queries.
        }

        @Override
        public void visit(XMLSerializeExpr expr) {
            if (expr.getExpression() != null) {
                expr.getExpression().accept(this);
            }
        }
    }

}
