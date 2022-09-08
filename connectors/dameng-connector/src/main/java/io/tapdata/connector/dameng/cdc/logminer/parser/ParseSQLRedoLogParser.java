package io.tapdata.connector.dameng.cdc.logminer.parser;

import io.tapdata.common.cdc.RedoLogContent;
import org.parboiled.Node;
import org.parboiled.Parboiled;
import org.parboiled.Rule;
import org.parboiled.parserunners.BasicParseRunner;
import org.parboiled.support.ParseTreeUtils;
import org.parboiled.support.ParsingResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ParseSQLRedoLogParser implements RedoLogParser {

    private static final ThreadLocal<OracleSQLParser> SQL_PARSER_THREAD_LOCAL = ThreadLocal.withInitial(() -> Parboiled.createParser(OracleSQLParser.class));

/*  public static RuleContextAndOpCode getRuleContextAndCode(String queryString, int op) {
    plsqlLexer lexer = new plsqlLexer(new ANTLRInputStream(queryString));
    CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    plsqlParser parser = new plsqlParser(tokenStream);
    RuleContextAndOpCode contextAndOpCode = new RuleContextAndOpCode();
    switch (op) {
      case UPDATE_CODE:
      case SELECT_FOR_UPDATE_CODE:
        contextAndOpCode.context = parser.update_statement();
        ;
        contextAndOpCode.operationCode = ConnectorConstant.MESSAGE_OPERATION_UPDATE;
        break;
      case INSERT_CODE:
        contextAndOpCode.context = parser.insert_statement();
        contextAndOpCode.operationCode = ConnectorConstant.MESSAGE_OPERATION_INSERT;
        break;
      case DELETE_CODE:
        contextAndOpCode.context = parser.delete_statement();
        contextAndOpCode.operationCode = ConnectorConstant.MESSAGE_OPERATION_DELETE;
        break;
      case DDL_CODE:
      case COMMIT_CODE:
      case ROLLBACK_CODE:
        break;
      default:
        return null;
    }
    return contextAndOpCode;
  }*/

    @Override
    public Map<String, Object> parseSQL(String sql, RedoLogContent.OperationEnum operationEnum) {
        final OracleSQLParser sqlParser = SQL_PARSER_THREAD_LOCAL.get();
        Rule rule;
        switch (operationEnum) {
            case INSERT: {
                rule = sqlParser.Insert();
                break;
            }
            case UPDATE: {
                rule = sqlParser.Update();
                break;
            }
            case DELETE: {
                rule = sqlParser.Delete();
                break;
            }
            default:
                throw new UnsupportedOperationException(String.format("OperationCode [%s] is not supported for OracleSQLParser", operationEnum.getOperation()));
        }
        final ParsingResult<Object> result = new BasicParseRunner<>(rule).run(sql);
        if (!result.matched) {
            throw new IllegalArgumentException("OracleSQLParser parse sql fail: " + sql);
        }
        List<Node<Object>> names = new ArrayList<>();
        ParseTreeUtils.collectNodes(
                result.parseTreeRoot,
                input -> input.getLabel().equals(OracleSQLParser.COLUMN_NAME_RULE),
                names
        );
        List<Node<Object>> values = new ArrayList<>();
        ParseTreeUtils.collectNodes(
                result.parseTreeRoot,
                input -> input.getLabel().equals(OracleSQLParser.COLUMN_VALUE_RULE),
                values
        );
        final Map<String, Object> record = new HashMap<>();
        for (int i = 0; i < names.size(); i++) {
            Node<?> name = names.get(i);
            Node<?> val = values.get(i);
            final String colName = sql.substring(name.getStartIndex(), name.getEndIndex());
            final String key = formatName(colName, true); // this.sqlListener.setCaseSensitive(); is true
            if (!record.containsKey(key)) {
                record.put(key, formatValue(sql.substring(val.getStartIndex(), val.getEndIndex())));
            }
        }
        return record;
    }

    /**
     * Format column names based on whether they are case-sensitive
     */
    private static String formatName(String columnName, boolean caseSensitive) {
        String returnValue = format(columnName);
        if (caseSensitive) {
            return returnValue;
        }
        return returnValue.toUpperCase();
    }

    /**
     * Unescapes strings and returns them.
     */
    private static String formatValue(String value) {
        // The value can either be null (if the IS keyword is present before it or just a NULL string with no quotes)
        if (value == null || "NULL".equalsIgnoreCase(value)) {
            return null;
        } else if ("EMPTY_BLOB()".equals(value) || "EMPTY_CLOB()".equals(value)) {
            return null;
        }
        String returnValue = format(value);
        return returnValue.replaceAll("''", "'");
    }

    public static String format(String columnName) {
        int stripCount;

        if (columnName.startsWith("\"'")) {
            stripCount = 2;
        } else if (columnName.startsWith("\"") || columnName.startsWith("'")) {
            stripCount = 1;
        } else {
            return columnName;
        }
        return columnName.substring(stripCount, columnName.length() - stripCount);
    }
}
