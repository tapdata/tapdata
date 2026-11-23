package com.tapdata.tm.commons.inspect;
import org.apache.commons.collections4.CollectionUtils;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.Token;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.WindowElement;
import net.sf.jsqlparser.expression.WindowOffset;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
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
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import net.sf.jsqlparser.util.TablesNamesFinder;
import java.util.List;
import java.util.Locale;
import java.util.Arrays;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NextValExpression;
import net.sf.jsqlparser.schema.Column;
/** Rejects writes and known unsafe built-ins; database privileges must also restrict user-defined functions. */
public final class CustomQueryReadOnly {
    public static void validateReadOnlyQuery(String sql, String connectionName) {
        try {
            Statements statements = CCJSqlParserUtil.parseStatements(sql);
            rejectExecutableComments(sql);
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

    private static void rejectExecutableComments(String sql) {
        // Reuse the parser's lexer so quoted values/identifiers and ordinary comment text stay legal.
        // MySQL and MariaDB can execute comments that the generic AST deliberately ignores.
        CCJSqlParser lexer = CCJSqlParserUtil.newParser(sql);
        Token token;
        do {
            token = lexer.getNextToken();
            for (Token comment = token.specialToken; comment != null; comment = comment.specialToken) {
                if (comment.image.startsWith("/*!") || comment.image.startsWith("/*M!")) {
                    throw new IllegalArgumentException("Executable SQL comments are not allowed");
                }
            }
        } while (token.kind != 0);
    }

    private static class ReadOnlySelectValidator extends TablesNamesFinder {
        @Override
        public void visit(Function function) {
            // Include quoted and schema-qualified built-ins. SQL statement kind alone is insufficient.
            for (String part : function.getMultipartName()) {
                validateFunctionName(part);
            }
            super.visit(function);
            visitOrderBy(function.getOrderByElements());
            if (function.getNamedParameters() != null) function.getNamedParameters().getExpressions().forEach(e -> e.accept(this));
            visitExpression(function.getAttribute());
            if (function.getKeep() != null) function.getKeep().accept(this);
        }

        private void validateFunctionName(String part) {
            String name = part.replace("\"", "").replace("`", "").replace("[", "").replace("]", "").toLowerCase(Locale.ROOT);
            if (Arrays.asList("nextval", "setval", "set_config", "pg_terminate_backend", "pg_cancel_backend",
                    "pg_reload_conf", "pg_rotate_logfile", "pg_log_backend_memory_contexts", "pg_promote",
                    "get_lock", "release_lock", "release_all_locks", "sys_exec", "sys_eval",
                    "load_file", "pg_read_file", "pg_read_binary_file", "pg_ls_dir", "pg_stat_file",
                    "openrowset", "openquery", "opendatasource",
                    "file", "url", "s3", "remote", "remotesecure", "jdbc", "mysql", "postgresql",
                    "mongodb", "hdfs", "odbc", "input",
                    "pg_drop_replication_slot", "pg_create_physical_replication_slot",
                    "pg_create_logical_replication_slot", "pg_replication_slot_advance", "pg_switch_wal",
                    "pg_wal_replay_pause", "pg_wal_replay_resume", "pg_backup_start", "pg_backup_stop",
                    "pg_start_backup", "pg_stop_backup").contains(name)
                    || name.startsWith("dblink") || name.startsWith("lo_") || name.startsWith("pg_advisory_")
                    || name.startsWith("pg_try_advisory_") || name.startsWith("pg_write_") || name.startsWith("pg_ls_")
                    || name.startsWith("dbms_") || name.startsWith("utl_") || name.startsWith("xp_")) {
                throw new IllegalArgumentException("Side-effecting SQL function is not allowed: " + part);
            }
        }

        @Override
        public void visit(NextValExpression expression) {
            throw new IllegalArgumentException("Sequence mutation is not allowed");
        }

        @Override
        public void visit(Column column) {
            if ("nextval".equalsIgnoreCase(column.getColumnName().replace("\"", "")))
                throw new IllegalArgumentException("Sequence mutation is not allowed");
            super.visit(column);
        }

        @Override
        public void visit(PlainSelect plainSelect) {
            if (CollectionUtils.isNotEmpty(plainSelect.getIntoTables()) || plainSelect.isForUpdate()) {
                throw new IllegalArgumentException("SELECT INTO and FOR UPDATE are not allowed");
            }
            super.visit(plainSelect);
            // TablesNamesFinder 4.3 skips these expressions, including their nested SELECTs.
            visitJoinConditions(plainSelect.getJoins());
            visitOrderBy(plainSelect.getOrderByElements());
            visitRowLimits(plainSelect.getLimit(), plainSelect.getOffset());
            // FETCH in 4.3 only accepts a long or JDBC parameter; it has no SQL expression to walk.
            if (plainSelect.getTop() != null) visitExpression(plainSelect.getTop().getExpression());
            if (plainSelect.getDistinct() != null && plainSelect.getDistinct().getOnSelectItems() != null) {
                plainSelect.getDistinct().getOnSelectItems().forEach(item -> item.accept(this));
            }
            if (plainSelect.getGroupBy() != null && plainSelect.getGroupBy().getGroupByExpressions() != null) {
                plainSelect.getGroupBy().getGroupByExpressions().forEach(expression -> expression.accept(this));
            }
            if (plainSelect.getGroupBy() != null && plainSelect.getGroupBy().getGroupingSets() != null) {
                for (Object set : plainSelect.getGroupBy().getGroupingSets()) {
                    if (set instanceof Expression) visitExpression((Expression) set);
                    else if (set instanceof ExpressionList && ((ExpressionList) set).getExpressions() != null) {
                        ((ExpressionList) set).getExpressions().forEach(this::visitExpression);
                    }
                }
            }
        }

        @Override
        public void visit(SetOperationList setOperations) {
            super.visit(setOperations);
            visitOrderBy(setOperations.getOrderByElements());
            visitRowLimits(setOperations.getLimit(), setOperations.getOffset());
        }

        private void visitRowLimits(Limit limit, Offset offset) {
            if (limit != null) {
                visitExpression(limit.getRowCount());
                visitExpression(limit.getOffset());
            }
            if (offset != null) visitExpression(offset.getOffset());
        }

        @Override
        public void visit(AnalyticExpression analytic) {
            // TablesNamesFinder 4.3 does not walk analytic nodes at all, including FILTER/WITHIN GROUP.
            for (String part : analytic.getName().split("\\.")) validateFunctionName(part);
            visitExpression(analytic.getExpression());
            visitExpression(analytic.getOffset());
            visitExpression(analytic.getDefaultValue());
            visitExpression(analytic.getFilterExpression());
            if (analytic.getPartitionExpressionList() != null) analytic.getPartitionExpressionList().accept(this);
            visitOrderBy(analytic.getOrderByElements());
            visitOrderBy(analytic.getFuncOrderBy());
            if (analytic.getKeep() != null) analytic.getKeep().accept(this);
            WindowElement window = analytic.getWindowElement();
            if (window != null) {
                visitWindowOffset(window.getOffset());
                if (window.getRange() != null) {
                    visitWindowOffset(window.getRange().getStart());
                    visitWindowOffset(window.getRange().getEnd());
                }
            }
        }

        private void visitWindowOffset(WindowOffset offset) {
            if (offset != null) visitExpression(offset.getExpression());
        }

        private void visitExpression(Expression expression) {
            if (expression != null) expression.accept(this);
        }

        @Override
        public void visit(KeepExpression keep) {
            visitOrderBy(keep.getOrderByElements());
        }

        @Override
        public void visit(MySQLGroupConcat groupConcat) {
            if (groupConcat.getExpressionList() != null) groupConcat.getExpressionList().accept(this);
            visitOrderBy(groupConcat.getOrderByElements());
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
