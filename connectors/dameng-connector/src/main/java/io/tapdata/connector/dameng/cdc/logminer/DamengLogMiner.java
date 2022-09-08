package io.tapdata.connector.dameng.cdc.logminer;

import io.tapdata.common.cdc.ILogMiner;
import io.tapdata.common.cdc.LogMiner;
import io.tapdata.common.cdc.RedoLogContent;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.connector.dameng.DamengConfig;
import io.tapdata.connector.dameng.DamengContext;
import io.tapdata.connector.dameng.cdc.DamengOffset;
import io.tapdata.connector.dameng.cdc.logminer.bean.RedoLog;
import io.tapdata.connector.dameng.cdc.logminer.handler.DateTimeColumnHandler;
import io.tapdata.connector.dameng.cdc.logminer.handler.RawTypeHandler;
import io.tapdata.connector.dameng.cdc.logminer.handler.UnicodeStringColumnHandler;
import io.tapdata.connector.dameng.cdc.logminer.sqlparser.impl.DamengCDCSQLParser;
import io.tapdata.constant.SqlConstant;
import io.tapdata.constant.TapLog;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.tapdata.connector.dameng.cdc.logminer.constant.OracleSqlConstant.*;


public abstract class DamengLogMiner extends LogMiner implements ILogMiner {

    private final static String TAG = DamengLogMiner.class.getSimpleName();

    protected final static int TIMESTAMP_TZ_TYPE = -101;
    protected final static int TIMESTAMP_LTZ_TYPE = -102;

    protected static final String NLS_DATE_FORMAT = "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'";
    protected static final String NLS_NUMERIC_FORMAT = "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'";
    protected static final String NLS_TIMESTAMP_FORMAT = "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'";
    protected static final String NLS_TIMESTAMP_TZ_FORMAT = "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'";

    protected final DateTimeColumnHandler dateTimeColumnHandler;
    protected final DamengContext damengContext;
    protected final DamengConfig damengConfig;
    protected Statement statement;
    protected PreparedStatement preparedStatement;
    protected ResultSet resultSet;
    protected final String version;

    protected Map<String, Integer> columnTypeMap = new HashMap<>(); //Map<table.column, java.dataType>
    protected Map<String, String> dateTimeTypeMap = new HashMap<>(); //Map<table.column, db.dataType>

    protected DamengOffset damengOffset;

    public DamengLogMiner(DamengContext damengContext) {
        this.damengContext = damengContext;
        damengConfig = (DamengConfig) damengContext.getConfig();
        dateTimeColumnHandler = new DateTimeColumnHandler(damengConfig.getSysZoneId());
        version = damengContext.queryVersion();
        ddlParserType = DDLParserType.ORACLE_CCJ_SQL_PARSER;
    }

    //init with pdk params
    @Override
    public void init(List<String> tableList, KVReadOnlyMap<TapTable> tableMap, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        super.init(tableList, tableMap, offsetState, recordSize, consumer);
        makeOffset(offsetState);
        getColumnType();
    }

    //store dataType in Map
    private void getColumnType() {
        tableList.forEach(table -> {
            try {
                damengContext.queryWithNext("SELECT * FROM \"" + damengConfig.getSchema() + "\".\"" + table + "\"", resultSet -> {
                    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                        int colType = resultSetMetaData.getColumnType(i);
                        columnTypeMap.put(table + "." + resultSetMetaData.getColumnName(i), colType);
                        if (colType == Types.DATE || colType == Types.TIME || colType == Types.TIMESTAMP) {
                            dateTimeTypeMap.put(table + "." + resultSetMetaData.getColumnName(i), resultSetMetaData.getColumnTypeName(i));
                        }
                    }
                });
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * find current scn of database
     *
     * @return scn Long
     * @throws Throwable SQLException
     */
    private long findCurrentScn() throws Throwable {
        AtomicLong currentScn = new AtomicLong();
        String sql =  CHECK_CURRENT_SCN;
        damengContext.queryWithNext(sql, resultSet -> currentScn.set(resultSet.getLong(1)));
        return currentScn.get();
    }


    private void makeOffset(Object offsetState) throws Throwable {
        if (EmptyKit.isNull(offsetState)) {
            damengOffset = new DamengOffset();
            long currentScn = findCurrentScn();
            damengOffset.setLastScn(currentScn);
            damengOffset.setPendingScn(currentScn);
        } else {
            damengOffset = (DamengOffset) offsetState;
            if (EmptyKit.isNull(damengOffset.getLastScn())) {
                    long currentScn = findCurrentScn();
                    damengOffset.setLastScn(currentScn);
                    damengOffset.setPendingScn(currentScn);
                }
            }
        }


    protected RedoLog firstOnlineRedoLog(long scn) throws Throwable {
        AtomicReference<RedoLog> redoLog = new AtomicReference<>();
        boolean useOldVersionSql = StringUtils.equalsAnyIgnoreCase(version, "9i", "10g");
        String firstOnlineSQL = useOldVersionSql ? GET_FIRST_ONLINE_REDO_LOG_FILE_FOR_10G_AND_9I : GET_FIRST_ONLINE_REDO_LOG_FILE;
        if (scn > 0) {
            firstOnlineSQL = useOldVersionSql ? String.format(GET_FIRST_ONLINE_REDO_LOG_FILE_BY_SCN_FOR_10G_AND_9I, scn) : String.format(GET_FIRST_ONLINE_REDO_LOG_FILE_BY_SCN, scn);
        }
        damengContext.queryWithNext(firstOnlineSQL, resultSet -> redoLog.set(RedoLog.onlineLog(resultSet)));
        return redoLog.get();
    }

    protected void setSession() throws SQLException {
        statement = damengContext.getConnection().createStatement();
        if (EmptyKit.isNotBlank(damengConfig.getPdb())) {
            TapLogger.info(TAG, "database is containerised, switching...");
            statement.execute(SWITCH_TO_CDB_ROOT);
        }
        //statement.execute(NLS_DATE_FORMAT);
//        statement.execute(NLS_TIMESTAMP_FORMAT);
//        statement.execute(NLS_TIMESTAMP_TZ_FORMAT);
//        statement.execute(NLS_NUMERIC_FORMAT);
    }

    protected void initRedoLogQueueAndThread() {
        if (redoLogConsumerThreadPool == null) {
            redoLogConsumerThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            redoLogConsumerThreadPool.submit(() -> {
                RedoLogContent redoLogContent;
                while (isRunning.get()) {
                    while (ddlStop.get()) {
                        TapSimplify.sleep(1000);
                    }
                    try {
                        redoLogContent = logQueue.poll(1, TimeUnit.SECONDS);
                        if (redoLogContent == null) {
                            continue;
                        }
                    } catch (Exception e) {
                        break;
                    }
                    try {
                        // parse sql
                        if (canParse(redoLogContent)) {
                            RedoLogContent.OperationEnum operationEnum = RedoLogContent.OperationEnum.fromOperationCode(redoLogContent.getOperationCode());
                            String sqlRedo;
                            if (operationEnum == RedoLogContent.OperationEnum.DELETE) {
                                sqlRedo = redoLogContent.getSqlUndo();
                                operationEnum = RedoLogContent.OperationEnum.INSERT;
                            } else {
                                sqlRedo = redoLogContent.getSqlRedo();
                            }
                            redoLogContent.setRedoRecord(new DamengCDCSQLParser().from(sqlRedo).getData());
                            convertStringToObject(redoLogContent);
                        }
                        // process and callback
                        processOrBuffRedoLogContent(redoLogContent, this::sendTransaction);

                    } catch (Throwable e) {
                        e.printStackTrace();
                        consumer.streamReadEnded();
                    }
                }
            });
        }
    }

    /**
     * convert log redo string to Object(jdbc)
     *
     * @param redoLogContent oracle log content
     */
    protected void convertStringToObject(RedoLogContent redoLogContent) throws Throwable {
        String table = redoLogContent.getTableName();
        if (lobTables.containsKey(table)) {
            Collection<String> unique = lobTables.get(table).primaryKeys(true);
            Map<String, Object> redo = redoLogContent.getRedoRecord();
            preparedStatement = damengContext.getConnection().prepareStatement(
                    "SELECT * FROM \"" + damengConfig.getSchema() + "\".\"" + table + "\" WHERE " + unique.stream().map(v -> "\"" + v + "\"=?").collect(Collectors.joining(" AND ")));
            int pos = 1;
            for (String field : unique) {
                preparedStatement.setObject(pos++, redo.get(field));
            }
            ResultSet rs = preparedStatement.executeQuery();
            if (rs.next()) {
                redoLogContent.setRedoRecord(DbKit.getRowFromResultSet(rs, DbKit.getColumnsFromResultSet(rs)));
            }
            return;
        }
        redoLogContent.getRedoRecord().remove("ROWID");
        for (Map.Entry<String, Object> stringObjectEntry : redoLogContent.getRedoRecord().entrySet()) {
            Object value = stringObjectEntry.getValue();
            String column = stringObjectEntry.getKey();
            Integer columnType = columnTypeMap.get(table + "." + column);
            if (EmptyKit.isNull(value) || !(value instanceof String) || EmptyKit.isNull(columnType)) {
                continue;
            }
            switch (columnType) {
                case Types.BIGINT:
                    stringObjectEntry.setValue(new BigDecimal((String) value).longValue());
                    break;
                case Types.BINARY:
                case Types.LONGVARBINARY:
                case Types.VARBINARY:
                    try {
                        stringObjectEntry.setValue(RawTypeHandler.parseRaw((String) value));
                    } catch (DecoderException e) {
                        TapLogger.warn(TAG, TapLog.W_CONN_LOG_0014.getMsg(), value, columnType, e.getMessage());
                    }
                    break;
                case Types.BIT:
                case Types.BOOLEAN:
                    stringObjectEntry.setValue(Boolean.valueOf((String) value));
                    break;
                case Types.CHAR:
                case Types.LONGNVARCHAR:
                case Types.LONGVARCHAR:
                case Types.VARCHAR:
                case Types.ROWID:
                case Types.ARRAY:
                case Types.DATALINK:
                case Types.DISTINCT:
                case Types.JAVA_OBJECT:
                case Types.NULL:
                case Types.OTHER:
                case Types.REF:
                case Types.REF_CURSOR:
                case Types.SQLXML:
                case Types.STRUCT:
                case Types.TIME_WITH_TIMEZONE:
                    break;
                case Types.NCHAR:
                case Types.NVARCHAR:
                    stringObjectEntry.setValue(UnicodeStringColumnHandler.getUnicdeoString((String) value));
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                case Types.DOUBLE:
                    stringObjectEntry.setValue(new BigDecimal((String) value).doubleValue());
                    break;
                case Types.FLOAT:
                case Types.REAL:
                    stringObjectEntry.setValue(new BigDecimal((String) value).floatValue());
                    break;
                case Types.INTEGER:
                    stringObjectEntry.setValue(new BigDecimal((String) value).intValue());
                    break;
                case Types.SMALLINT:
                case Types.TINYINT:
                    stringObjectEntry.setValue(new BigDecimal((String) value).shortValue());
                    break;
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    String actualType = dateTimeTypeMap.get(table + "." + column);
                    if (StringUtils.contains((CharSequence) value, "::")) {
                        stringObjectEntry.setValue(dateTimeColumnHandler.getTimestamp(value, actualType));
                    } else {
                        // For whatever reason, Oracle returns all the date/time/timestamp fields as the same type, so additional
                        // logic is required to accurately parse the type
                        stringObjectEntry.setValue(dateTimeColumnHandler.getDateTimeStampField((String) value, actualType));
                    }
                    break;
                case Types.TIMESTAMP_WITH_TIMEZONE:
                case TIMESTAMP_TZ_TYPE:
                    String tzDateType = dateTimeTypeMap.get(table + "." + column);
                    if (StringUtils.contains((CharSequence) value, "::")) {
                        stringObjectEntry.setValue(dateTimeColumnHandler.getTimestamp(value, tzDateType));
                    } else {
                        stringObjectEntry.setValue(dateTimeColumnHandler.getTimestampWithTimezoneField((String) value));
                    }
                    break;
                case TIMESTAMP_LTZ_TYPE:
                    String ltzDateType = dateTimeTypeMap.get(table + "." + column);
                    if (StringUtils.contains((CharSequence) value, "::")) {
                        stringObjectEntry.setValue(dateTimeColumnHandler.getTimestamp(value, ltzDateType));
                    } else {
                        stringObjectEntry.setValue(dateTimeColumnHandler.getTimestampWithLocalTimezone((String) value));
                    }
                    break;
                case Types.BLOB:
                case Types.CLOB:
                case Types.NCLOB:
                    if ("EMPTY_BLOB()".equals(value)) {
                        stringObjectEntry.setValue(null);
                    }
                    break;
            }
        }
    }

    private boolean canParse(RedoLogContent redoLogContent) {
        if (redoLogContent == null) {
            return false;
        }
        if (redoLogContent.getUndoRecord() != null || redoLogContent.getRedoRecord() != null) {
            return false;
        }
        switch (redoLogContent.getOperation()) {
            case SqlConstant.REDO_LOG_OPERATION_LOB_TRIM:
            case SqlConstant.REDO_LOG_OPERATION_LOB_WRITE:
            case SqlConstant.REDO_LOG_OPERATION_SEL_LOB_LOCATOR:
            case "INTERNAL":
                return false;
            default:
                break;
        }
        String sqlRedo = redoLogContent.getSqlRedo();
        String sqlUndo = redoLogContent.getSqlUndo();
        if (StringUtils.isAllBlank(sqlRedo, sqlUndo)) {
            return false;
        }
        String operation = redoLogContent.getOperation();
        if (!StringUtils.equalsAny(operation,
                SqlConstant.REDO_LOG_OPERATION_INSERT,
                SqlConstant.REDO_LOG_OPERATION_UPDATE,
                SqlConstant.REDO_LOG_OPERATION_DELETE)) {
            return false;
        }
        return !StringUtils.equalsAny(operation, SqlConstant.REDO_LOG_OPERATION_DELETE)
                || !StringUtils.isEmpty(redoLogContent.getSqlUndo());
    }

    @Override
    protected void ddlFlush() {
        columnTypeMap.clear();
        dateTimeTypeMap.clear();
        getColumnType();
        makeLobTables();
    }

    @Override
    protected void submitEvent(RedoLogContent redoLogContent, List<TapEvent> eventList) {
        DamengOffset oracleOffset = new DamengOffset();
        assert redoLogContent != null;
        oracleOffset.setLastScn(redoLogContent.getScn());
        oracleOffset.setPendingScn(redoLogContent.getScn());
        oracleOffset.setTimestamp(redoLogContent.getTimestamp().getTime());
        if (eventList.size() > 0) {
            consumer.accept(eventList, oracleOffset);
        }
    }

    protected String analyzeLogSql(Long scn) {
        String sql = GET_REDO_LOG_RESULT_ORACLE_LOG_COLLECT_SQL;
        sql = String.format(
                    sql,
                    scn,
                    " AND SEG_OWNER='" + damengConfig.getSchema() + "'",
                    " AND TABLE_NAME IN (" + StringKit.joinString(tableList, "'", ", ") + ")"
            );
        return sql;
    }

    protected void analyzeLog(Object logData) throws SQLException {
        RedoLogContent redoLogContent = wrapRedoLogContent(logData);
        if (!validateRedoLogContent(redoLogContent)) {
            return;
        }
        if (csfRedoLogProcess(logData, redoLogContent)) {
            return;
        }
        String operation = redoLogContent.getOperation();
        if (SqlConstant.REDO_LOG_OPERATION_LOB_TRIM.equals(operation)
                || SqlConstant.REDO_LOG_OPERATION_LOB_WRITE.equals(operation)) {
            return;
        }
        if (SqlConstant.REDO_LOG_OPERATION_UNSUPPORTED.equals(operation)) {
            return;
        }
        enqueueRedoLogContent(redoLogContent);
    }

    private RedoLogContent wrapRedoLogContent(Object logData) throws SQLException {
        if (csfLogContent == null) {
            return buildRedoLogContent(logData);
        } else {
            return appendRedoAndUndoSql(logData);
        }
    }

    private RedoLogContent buildRedoLogContent(Object logData) throws SQLException {
        RedoLogContent redoLogContent;
        if (logData instanceof ResultSet) {
            redoLogContent = new RedoLogContent(resultSet, damengConfig.getSysZoneId());

        } else if (logData instanceof Map) {
            redoLogContent = new RedoLogContent((Map) logData);
        } else {
            redoLogContent = null;
        }
        return redoLogContent;
    }

    private RedoLogContent appendRedoAndUndoSql(Object logData) throws SQLException {
        if (logData == null) {
            return null;
        }

        String redoSql = "";
        String undoSql = "";

        if (logData instanceof ResultSet) {
            redoSql = ((ResultSet) logData).getString("SQL_REDO");
            undoSql = ((ResultSet) logData).getString("SQL_UNDO");
        } else if (logData instanceof Map) {
            Object sqlRedoObj = ((Map) logData).getOrDefault("SQL_REDO", "");
            if (sqlRedoObj != null) {
                redoSql = sqlRedoObj.toString();
            }
            final Object sqlUndoObj = ((Map) logData).getOrDefault("SQL_UNDO", "");
            if (sqlUndoObj != null) {
                undoSql = sqlUndoObj.toString();
            }
        }
        if (StringUtils.isNotBlank(redoSql)) {
            csfLogContent.setSqlRedo(csfLogContent.getSqlRedo() + redoSql);
        }

        if (StringUtils.isNotBlank(undoSql)) {
            csfLogContent.setSqlUndo(csfLogContent.getSqlUndo() + undoSql);
        }

        RedoLogContent redoLogContent = new RedoLogContent();
        beanUtils.copyProperties(csfLogContent, redoLogContent);

        return redoLogContent;
    }

    private boolean validateRedoLogContent(RedoLogContent redoLogContent) {
        if (redoLogContent == null) {
            return false;
        }

        if (!StringUtils.equalsAnyIgnoreCase(redoLogContent.getOperation(),
                SqlConstant.REDO_LOG_OPERATION_COMMIT, SqlConstant.REDO_LOG_OPERATION_ROLLBACK)) {
            // check owner
            if (StringUtils.isNotBlank(redoLogContent.getSegOwner())
                    && !damengConfig.getSchema().equals(redoLogContent.getSegOwner())) {
                return false;
            }
            // check table name
            return !EmptyKit.isNotBlank(redoLogContent.getTableName()) || tableList.contains(redoLogContent.getTableName());
        }

        return true;
    }

    private boolean csfRedoLogProcess(Object logData, RedoLogContent redoLogContent) {
        // handle continuation redo/undo sql
        if (isCsf(logData)) {
            if (csfLogContent == null) {
                csfLogContent = new RedoLogContent();
                beanUtils.copyProperties(redoLogContent, csfLogContent);
            }
            return true;
        } else {
            csfLogContent = null;
        }
        return false;
    }

    private static boolean isCsf(Object logData) {
        if (logData != null) {
            try {
                Integer csf = null;

                if (logData instanceof ResultSet) {
                    csf = ((ResultSet) logData).getInt("CSF");
                } else if (logData instanceof Map) {
                    csf = Integer.valueOf(((Map) logData).get("CSF").toString());
                }

                if (csf != null) {
                    return csf.equals(1);
                } else {
                    return false;
                }
            } catch (Exception e) {
                return false;
            }
        }

        return false;
    }
    @Override
    public abstract void startMiner() throws Throwable;
    @Override
    public void stopMiner() throws Throwable {
        super.stopMiner();
        if (EmptyKit.isNotNull(statement)) {
            statement.execute(END_LOG_MINOR_SQL);
            statement.close();
        }
        if (EmptyKit.isNotNull(resultSet)) {
            resultSet.close();
            resultSet = null;
        }
        if (EmptyKit.isNotNull(preparedStatement)) {
            preparedStatement.close();
        }
    }
}
