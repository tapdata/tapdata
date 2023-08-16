package io.tapdata.common;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.ddl.DDLSqlGenerator;
import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.index.TapDeleteIndexEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.list;

public abstract class CommonDbConnector extends ConnectorBase {

    //SQL for Primary key sorting area reading
    private final static String FIND_KEY_FROM_OFFSET = "select * from (select %s, row_number() over (order by %s) as tap__rowno from %s ) a where tap__rowno=%s";
    private final static String wherePattern = "where %s ";
    //offset for Primary key sorting area reading
    private final static Long offsetSize = 1000000L;
    protected static final int BATCH_ADVANCE_READ_LIMIT = 1000;

    //ddlHandlers which for ddl collection
    protected BiClassHandlers<TapFieldBaseEvent, TapConnectorContext, List<String>> fieldDDLHandlers;
    //ddlSqlMaker which for ddl execution
    protected DDLSqlGenerator ddlSqlGenerator;
    //Once the task is started, this ID is a unique identifier and stored in the stateMap
    protected String firstConnectorId;
    //jdbc context for each relation datasource
    protected JdbcContext jdbcContext;
    //db config for each relation datasource (load properties from TapConnectionContext)
    protected CommonDbConfig commonDbConfig;
    protected CommonSqlMaker commonSqlMaker;
    protected Log tapLogger;
    protected ExceptionCollector exceptionCollector = new AbstractExceptionCollector() {
    };
    protected Map<String, Connection> transactionConnectionMap = new ConcurrentHashMap<>();
    protected boolean isTransaction = false;

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws SQLException {
        return jdbcContext.queryAllTables(null).size();
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws SQLException {
        List<DataMap> tableList = jdbcContext.queryAllTables(tables);
        multiThreadDiscoverSchema(tableList, tableSize, consumer);
    }

    @Override
    protected void singleThreadDiscoverSchema(List<DataMap> subList, Consumer<List<TapTable>> consumer) throws SQLException {
        List<TapTable> tapTableList = TapSimplify.list();
        List<String> subTableNames = subList.stream().map(v -> v.getString("tableName")).collect(Collectors.toList());
        List<DataMap> columnList = jdbcContext.queryAllColumns(subTableNames);
        List<DataMap> indexList = jdbcContext.queryAllIndexes(subTableNames);
        subList.forEach(subTable -> {
            //2、table name/comment
            String table = subTable.getString("tableName");
            TapTable tapTable = table(table);
            tapTable.setComment(subTable.getString("tableComment"));
            //3、primary key and table index
            List<String> primaryKey = TapSimplify.list();
            List<TapIndex> tapIndexList = TapSimplify.list();
            makePrimaryKeyAndIndex(indexList, table, primaryKey, tapIndexList);
            //4、table columns info
            AtomicInteger keyPos = new AtomicInteger(0);
            columnList.stream().filter(col -> table.equals(col.getString("tableName")))
                    .forEach(col -> {
                        TapField tapField = makeTapField(col);
                        tapField.setPos(keyPos.incrementAndGet());
                        tapField.setPrimaryKey(primaryKey.contains(tapField.getName()));
                        tapField.setPrimaryKeyPos(primaryKey.indexOf(tapField.getName()) + 1);
                        if (tapField.getPrimaryKey()) {
                            tapField.setNullable(false);
                        }
                        tapTable.add(tapField);
                    });
            tapTable.setIndexList(tapIndexList);
            tapTableList.add(tapTable);
        });
        syncSchemaSubmit(tapTableList, consumer);
    }

    //some datasource makePrimaryKeyAndIndex in not the same way, such as db2
    protected void makePrimaryKeyAndIndex(List<DataMap> indexList, String table, List<String> primaryKey, List<TapIndex> tapIndexList) {
        Map<String, List<DataMap>> indexMap = indexList.stream().filter(idx -> table.equals(idx.getString("tableName")))
                .collect(Collectors.groupingBy(idx -> idx.getString("indexName"), LinkedHashMap::new, Collectors.toList()));
        indexMap.forEach((key, value) -> {
            if (value.stream().anyMatch(v -> ("1".equals(v.getString("isPk"))))) {
                primaryKey.addAll(value.stream().map(v -> v.getString("columnName")).collect(Collectors.toList()));
            }
            tapIndexList.add(makeTapIndex(key, value));
        });
    }

    protected TapField makeTapField(DataMap dataMap) {
        return new CommonColumn(dataMap).getTapField();
    }

    protected void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) throws SQLException {
        jdbcContext.queryAllTables(list(), batchSize, listConsumer);
    }

    private CreateTableOptions createTable(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent, Boolean commentInField) throws SQLException {
        TapTable tapTable = createTableEvent.getTable();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        if (jdbcContext.queryAllTables(Collections.singletonList(tapTable.getId())).size() > 0) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }

        Map<String, TapField> fieldMap = tapTable.getNameFieldMap();
        for (String field : fieldMap.keySet()) {
            String fieldDefault = (String) fieldMap.get(field).getDefaultValue();
            if (EmptyKit.isNotEmpty(fieldDefault)) {
                if (fieldDefault.contains("'")) {
                    fieldDefault = fieldDefault.replaceAll("'", "''");
                    fieldMap.get(field).setDefaultValue(fieldDefault);
                }
            }
        }
        List<String> sqlList = TapSimplify.list();
        sqlList.add(getCreateTableSql(tapTable, commentInField));
        if (!commentInField) {
            //comment on table and column
            if (EmptyKit.isNotNull(tapTable.getComment())) {
                sqlList.add(getTableCommentSql(tapTable));
            }
            for (String fieldName : fieldMap.keySet()) {
                TapField field = fieldMap.get(fieldName);
                String fieldComment = field.getComment();
                if (EmptyKit.isNotNull(fieldComment)) {
                    sqlList.add(getColumnCommentSql(tapTable, field));
                }
            }
        }
        try {
            jdbcContext.batchExecute(sqlList);
        } catch (SQLException e) {
            exceptionCollector.collectWritePrivileges("createTable", Collections.emptyList(), e);
            throw e;
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    //for pg,oracle type
    protected CreateTableOptions createTableV2(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws SQLException {
        return createTable(connectorContext, createTableEvent, false);
    }

    //for mysql type
    protected CreateTableOptions createTableV3(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws SQLException {
        return createTable(connectorContext, createTableEvent, true);
    }

    //Primary key sorting area reading
    protected void batchReadV3(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        List<String> primaryKeys = new ArrayList<>(tapTable.primaryKeys());
        char escapeChar = commonDbConfig.getEscapeChar();
        String selectClause = getSelectSql(tapTable);
        CommonDbOffset offset = (CommonDbOffset) offsetState;
        if (EmptyKit.isNull(offset)) {
            offset = new CommonDbOffset(new DataMap(), 0L);
        }
        if (EmptyKit.isEmpty(primaryKeys)) {
            submitInitialReadEvents(selectClause, tapTable, eventBatchSize, eventsOffsetConsumer, offset);
        } else {
            while (isAlive()) {
                DataMap from = offset.getColumnValue();
                DataMap to = findPrimaryKeyValue(tapTable, offset.getOffsetSize() + offsetSize);
                if (EmptyKit.isEmpty(from) && EmptyKit.isEmpty(to)) {
                    submitInitialReadEvents(selectClause, tapTable, eventBatchSize, eventsOffsetConsumer, offset);
                    break;
                } else if (EmptyKit.isEmpty(from) && EmptyKit.isNotEmpty(to)) {
                    for (int i = 0; i < primaryKeys.size(); i++) {
                        StringBuilder whereAppender = new StringBuilder();
                        whereAppender.append(primaryKeys.stream().limit(i).map(col -> escapeChar + col + escapeChar + "=?").collect(Collectors.joining(" and ")));
                        if (i > 0) {
                            whereAppender.append(" and ");
                        }
                        if (i == primaryKeys.size() - 1) {
                            whereAppender.append(escapeChar).append(primaryKeys.get(i)).append(escapeChar).append("<=?");
                        } else {
                            whereAppender.append(escapeChar).append(primaryKeys.get(i)).append(escapeChar).append("<?");
                        }
                        List<Object> params = primaryKeys.stream().limit(i + 1).map(to::get).collect(Collectors.toList());
                        submitOffsetReadEvents(selectClause + String.format(wherePattern, whereAppender), params, tapTable, eventBatchSize, eventsOffsetConsumer, offset);
                    }
                } else if (EmptyKit.isNotEmpty(from) && EmptyKit.isNotEmpty(to)) {
                    int sameKeySize = 0;
                    for (String key : primaryKeys) {
                        if (Objects.equals(from.get(key), to.get(key))) {
                            sameKeySize++;
                        } else {
                            break;
                        }
                    }
                    for (int i = primaryKeys.size() - 1; i > sameKeySize; i--) {
                        StringBuilder whereAppender = new StringBuilder();
                        whereAppender.append(primaryKeys.stream().limit(i).map(col -> escapeChar + col + escapeChar + "=?").collect(Collectors.joining(" and ")));
                        if (i > 0) {
                            whereAppender.append(" and ");
                        }
                        whereAppender.append(escapeChar).append(primaryKeys.get(i)).append(escapeChar).append(">?");
                        List<Object> params = primaryKeys.stream().limit(i + 1).map(from::get).collect(Collectors.toList());
                        submitOffsetReadEvents(selectClause + String.format(wherePattern, whereAppender), params, tapTable, eventBatchSize, eventsOffsetConsumer, offset);
                    }
                    StringBuilder whereAppenderMajor = new StringBuilder();
                    whereAppenderMajor.append(primaryKeys.stream().limit(sameKeySize).map(col -> escapeChar + col + escapeChar + "=?").collect(Collectors.joining(" and ")));
                    if (sameKeySize > 0) {
                        whereAppenderMajor.append(" and ");
                    }
                    whereAppenderMajor.append(escapeChar).append(primaryKeys.get(sameKeySize)).append(escapeChar).append(">? and ").append(escapeChar).append(primaryKeys.get(sameKeySize)).append(escapeChar).append("<?");
                    List<Object> paramsMajor = primaryKeys.stream().limit(sameKeySize + 1).map(from::get).collect(Collectors.toList());
                    paramsMajor.add(to.get(primaryKeys.get(sameKeySize)));
                    submitOffsetReadEvents(selectClause + String.format(wherePattern, whereAppenderMajor), paramsMajor, tapTable, eventBatchSize, eventsOffsetConsumer, offset);
                    for (int i = sameKeySize + 1; i <= primaryKeys.size(); i++) {
                        StringBuilder whereAppender = new StringBuilder();
                        whereAppender.append(primaryKeys.stream().limit(i).map(col -> escapeChar + col + escapeChar + "=?").collect(Collectors.joining(" and ")));
                        if (i < primaryKeys.size()) {
                            whereAppender.append(" and ").append(escapeChar).append(primaryKeys.get(i)).append(escapeChar).append("<?");
                        }
                        List<Object> params = primaryKeys.stream().limit(i).map(to::get).collect(Collectors.toList());
                        if (i < primaryKeys.size()) {
                            params.add(to.get(primaryKeys.get(i)));
                        }
                        submitOffsetReadEvents(selectClause + String.format(wherePattern, whereAppender), params, tapTable, eventBatchSize, eventsOffsetConsumer, offset);
                    }
                } else {
                    for (int i = primaryKeys.size() - 1; i >= 0; i--) {
                        StringBuilder whereAppender = new StringBuilder();
                        whereAppender.append(primaryKeys.stream().limit(i).map(col -> escapeChar + col + escapeChar + "=?").collect(Collectors.joining(" and ")));
                        if (i > 0) {
                            whereAppender.append(" and ");
                        }
                        whereAppender.append(escapeChar).append(primaryKeys.get(i)).append(escapeChar).append(">?");
                        List<Object> params = primaryKeys.stream().limit(i + 1).map(from::get).collect(Collectors.toList());
                        submitOffsetReadEvents(selectClause + String.format(wherePattern, whereAppender), params, tapTable, eventBatchSize, eventsOffsetConsumer, offset);
                    }
                    break;
                }
                offset = new CommonDbOffset(to, offset.getOffsetSize() + offsetSize);
            }
        }
    }

    private void submitInitialReadEvents(String sql, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, Object offset) throws Throwable {
        jdbcContext.query(sql, resultSet -> allOverResultSet(resultSet, tapTable, eventBatchSize, eventsOffsetConsumer, offset));
    }

    private void submitOffsetReadEvents(String prepareSql, List<Object> params, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, Object offset) throws Throwable {
        jdbcContext.prepareQuery(prepareSql, params, resultSet -> allOverResultSet(resultSet, tapTable, eventBatchSize, eventsOffsetConsumer, offset));
    }

    private void allOverResultSet(ResultSet resultSet, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, Object offset) throws SQLException {
        List<TapEvent> tapEvents = list();
        //get all column names
        List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
        try {
            while (isAlive() && resultSet.next()) {
                DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
                processDataMap(dataMap, tapTable);
                tapEvents.add(insertRecordEvent(dataMap, tapTable.getId()));
                if (tapEvents.size() == eventBatchSize) {
                    eventsOffsetConsumer.accept(tapEvents, offset);
                    tapEvents = list();
                }
            }
        } catch (SQLException e) {
            exceptionCollector.collectTerminateByServer(e);
            exceptionCollector.collectReadPrivileges("batchReadV3", Collections.emptyList(), e);
            throw e;
        }
        //last events those less than eventBatchSize
        if (EmptyKit.isNotEmpty(tapEvents)) {
            eventsOffsetConsumer.accept(tapEvents, offset);
        }
    }

    protected void processDataMap(DataMap dataMap, TapTable tapTable) throws RuntimeException {

    }

    private DataMap findPrimaryKeyValue(TapTable tapTable, Long offsetSize) throws Throwable {
        char escapeChar = commonDbConfig.getEscapeChar();
        String primaryKeyString = escapeChar + String.join(escapeChar + "," + escapeChar, tapTable.primaryKeys()) + escapeChar;
        DataMap dataMap = new DataMap();
        jdbcContext.query(String.format(FIND_KEY_FROM_OFFSET, primaryKeyString, primaryKeyString, getSchemaAndTable(tapTable.getId()), offsetSize), resultSet -> {
            if (resultSet.next()) {
                dataMap.putAll(DataMap.create(DbKit.getRowFromResultSet(resultSet, DbKit.getColumnsFromResultSet(resultSet))));
            }
        });
        return dataMap;
    }

    protected void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws SQLException {
        if (jdbcContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() == 1) {
            jdbcContext.execute("truncate table " + getSchemaAndTable(tapClearTableEvent.getTableId()));
        }
    }

    protected void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws SQLException {
        if (jdbcContext.queryAllTables(Collections.singletonList(tapDropTableEvent.getTableId())).size() == 1) {
            jdbcContext.execute("drop table " + getSchemaAndTable(tapDropTableEvent.getTableId()));
        }
    }

    protected long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        try {
            AtomicLong count = new AtomicLong(0);
            String sql = "select count(1) from " + getSchemaAndTable(tapTable.getId());
            jdbcContext.queryWithNext(sql, resultSet -> count.set(resultSet.getLong(1)));
            return count.get();
        } catch (SQLException e) {
            exceptionCollector.collectReadPrivileges("batchCount", Collections.emptyList(), e);
            throw e;
        }
    }

    //one filter can only match one record
    protected void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer) {
        Set<String> columnNames = tapTable.getNameFieldMap().keySet();
        List<FilterResult> filterResults = new LinkedList<>();
        for (TapFilter filter : filters) {
            String sql = "select * from " + getSchemaAndTable(tapTable.getId()) + " where " + commonSqlMaker.buildKeyAndValue(filter.getMatch(), "and", "=");
            FilterResult filterResult = new FilterResult();
            try {
                jdbcContext.query(sql, resultSet -> {
                    if (resultSet.next()) {
                        filterResult.setResult(DbKit.getRowFromResultSet(resultSet, columnNames));
                    }
                });
            } catch (Throwable e) {
                filterResult.setError(e);
            } finally {
                filterResults.add(filterResult);
            }
        }
        listConsumer.accept(filterResults);
    }

    protected void createIndex(TapConnectorContext connectorContext, TapTable tapTable, TapCreateIndexEvent createIndexEvent) throws SQLException {
        List<String> sqlList = TapSimplify.list();
        List<TapIndex> indexList = createIndexEvent.getIndexList()
                .stream()
                .filter(v -> discoverIndex(tapTable.getId())
                        .stream()
                        .noneMatch(i -> DbKit.ignoreCreateIndex(i, v)))
                .collect(Collectors.toList());
        if (EmptyKit.isNotEmpty(indexList)) {
            indexList.stream().filter(i -> !i.isPrimary()).forEach(i ->
                    sqlList.add(getCreateIndexSql(tapTable, i)));
        }
        jdbcContext.batchExecute(sqlList);
    }

    protected TapIndex makeTapIndex(String key, List<DataMap> value) {
        TapIndex index = new TapIndex();
        index.setName(key);
        List<TapIndexField> fieldList = TapSimplify.list();
        value.forEach(v -> {
            TapIndexField field = new TapIndexField();
            field.setFieldAsc("1".equals(v.getString("isAsc")));
            field.setName(v.getString("columnName"));
            fieldList.add(field);
        });
        index.setUnique(value.stream().anyMatch(v -> ("1".equals(v.getString("isUnique")))));
        index.setPrimary(value.stream().anyMatch(v -> ("1".equals(v.getString("isPk")))));
        index.setIndexFields(fieldList);
        return index;
    }

    protected List<TapIndex> discoverIndex(String tableName) {
        List<TapIndex> tapIndexList = TapSimplify.list();
        List<DataMap> indexList;
        try {
            indexList = jdbcContext.queryAllIndexes(Collections.singletonList(tableName));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Map<String, List<DataMap>> indexMap = indexList.stream()
                .collect(Collectors.groupingBy(idx -> idx.getString("indexName"), LinkedHashMap::new, Collectors.toList()));
        indexMap.forEach((key, value) -> tapIndexList.add(makeTapIndex(key, value)));
        return tapIndexList;
    }

    protected void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) throws SQLException {
        List<String> sqlList = fieldDDLHandlers.handle(tapFieldBaseEvent, tapConnectorContext);
        if (null == sqlList) {
            return;
        }
        jdbcContext.batchExecute(sqlList);
    }

    protected List<String> alterFieldAttr(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldAttributesEvent)) {
            return null;
        }
        TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = (TapAlterFieldAttributesEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.alterColumnAttr(commonDbConfig, tapAlterFieldAttributesEvent);
    }

    protected List<String> dropField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapDropFieldEvent)) {
            return null;
        }
        TapDropFieldEvent tapDropFieldEvent = (TapDropFieldEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.dropColumn(commonDbConfig, tapDropFieldEvent);
    }

    protected List<String> alterFieldName(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldNameEvent)) {
            return null;
        }
        TapAlterFieldNameEvent tapAlterFieldNameEvent = (TapAlterFieldNameEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.alterColumnName(commonDbConfig, tapAlterFieldNameEvent);
    }

    protected List<String> newField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapNewFieldEvent)) {
            return null;
        }
        TapNewFieldEvent tapNewFieldEvent = (TapNewFieldEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.addColumn(commonDbConfig, tapNewFieldEvent);
    }

    protected String getSchemaAndTable(String tableId) {
        StringBuilder sb = new StringBuilder();
        char escapeChar = commonDbConfig.getEscapeChar();
        if (EmptyKit.isNotBlank(commonDbConfig.getSchema())) {
            sb.append(escapeChar).append(commonDbConfig.getSchema()).append(escapeChar).append('.');
        }
        sb.append(escapeChar).append(tableId).append(escapeChar);
        return sb.toString();
    }

    private String getCreateTableSql(TapTable tapTable, Boolean commentInField) {
        char escapeChar = commonDbConfig.getEscapeChar();
        StringBuilder sb = new StringBuilder("create table ");
        sb.append(getSchemaAndTable(tapTable.getId())).append('(').append(commonSqlMaker.buildColumnDefinition(tapTable, commentInField));
        Collection<String> primaryKeys = tapTable.primaryKeys();
        if (EmptyKit.isNotEmpty(primaryKeys)) {
            sb.append(", primary key (").append(escapeChar)
                    .append(String.join(escapeChar + "," + escapeChar, primaryKeys))
                    .append(escapeChar).append(')');
        }
        sb.append(')');
        if (commentInField && EmptyKit.isNotBlank(tapTable.getComment())) {
            sb.append(" comment='").append(tapTable.getComment()).append("'");
        }
        return sb.toString();
    }

    protected String getCreateIndexSql(TapTable tapTable, TapIndex tapIndex) {
        StringBuilder sb = new StringBuilder("create ");
        char escapeChar = commonDbConfig.getEscapeChar();
        if (tapIndex.isUnique()) {
            sb.append("unique ");
        }
        sb.append("index ");
        if (EmptyKit.isNotBlank(tapIndex.getName())) {
            sb.append(escapeChar).append(tapIndex.getName()).append(escapeChar);
        } else {
            sb.append(escapeChar).append(DbKit.buildIndexName(tapTable.getId())).append(escapeChar);
        }
        sb.append(" on ").append(getSchemaAndTable(tapTable.getId())).append('(')
                .append(tapIndex.getIndexFields().stream().map(f -> escapeChar + f.getName() + escapeChar + " " + (f.getFieldAsc() ? "asc" : "desc"))
                        .collect(Collectors.joining(","))).append(')');
        return sb.toString();
    }

    private String getTableCommentSql(TapTable tapTable) {
        return "comment on table " + getSchemaAndTable(tapTable.getId()) +
                " is '" + tapTable.getComment() + '\'';
    }

    private String getColumnCommentSql(TapTable tapTable, TapField tapField) {
        char escapeChar = commonDbConfig.getEscapeChar();
        return "comment on column " + getSchemaAndTable(tapTable.getId()) + '.' +
                escapeChar + tapField.getName() + escapeChar +
                " is '" + tapField.getComment() + '\'';
    }

    private String getSelectSql(TapTable tapTable) {
        char escapeChar = commonDbConfig.getEscapeChar();
        return "select " + escapeChar + String.join(escapeChar + "," + escapeChar, tapTable.getNameFieldMap().keySet()) + escapeChar + " from " +
                getSchemaAndTable(tapTable.getId());
    }

    protected void runRawCommand(TapConnectorContext connectorContext, String command, TapTable tapTable, int eventBatchSize, Consumer<List<TapEvent>> eventsOffsetConsumer) throws Throwable {
        jdbcContext.query(command, resultSet -> {
            List<TapEvent> tapEvents = list();
            List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
            while (isAlive() && resultSet.next()) {
                DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
                tapEvents.add(insertRecordEvent(dataMap, tapTable.getId()));
                if (tapEvents.size() == eventBatchSize) {
                    eventsOffsetConsumer.accept(tapEvents);
                    tapEvents = list();
                }
            }
            if (EmptyKit.isNotEmpty(tapEvents)) {
                eventsOffsetConsumer.accept(tapEvents);
            }
        });
    }

    protected void batchReadWithoutOffset(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String columns = tapTable.getNameFieldMap().keySet().stream().map(c -> commonDbConfig.getEscapeChar() + c + commonDbConfig.getEscapeChar()).collect(Collectors.joining(","));
        String sql = String.format("SELECT %s FROM " + getSchemaAndTable(tapTable.getId()), columns);

        jdbcContext.query(sql, resultSet -> {
            List<TapEvent> tapEvents = list();
            //get all column names
            List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
            try {
                while (isAlive() && resultSet.next()) {
                    DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
                    processDataMap(dataMap, tapTable);
                    tapEvents.add(insertRecordEvent(dataMap, tapTable.getId()));
                    if (tapEvents.size() == eventBatchSize) {
                        eventsOffsetConsumer.accept(tapEvents, new HashMap<>());
                        tapEvents = list();
                    }
                }
            } catch (SQLException e) {
                exceptionCollector.collectTerminateByServer(e);
                exceptionCollector.collectReadPrivileges("batchReadWithoutOffset", Collections.emptyList(), e);
                exceptionCollector.revealException(e);
                throw e;
            }
            //last events those less than eventBatchSize
            if (EmptyKit.isNotEmpty(tapEvents)) {
                eventsOffsetConsumer.accept(tapEvents, new HashMap<>());
            }
        });
    }

    //for mysql type (with offset & limit)
    protected void queryByAdvanceFilterWithOffset(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        String sql = commonSqlMaker.buildSelectClause(table, filter) + getSchemaAndTable(table.getId()) + commonSqlMaker.buildSqlByAdvanceFilter(filter);
        jdbcContext.query(sql, resultSet -> {
            FilterResults filterResults = new FilterResults();
            try {
                while (resultSet.next()) {
                    List<String> allColumn = DbKit.getColumnsFromResultSet(resultSet);
                    filterResults.add(DbKit.getRowFromResultSet(resultSet, allColumn));
                    if (filterResults.getResults().size() == BATCH_ADVANCE_READ_LIMIT) {
                        consumer.accept(filterResults);
                        filterResults = new FilterResults();
                    }
                }
            } catch (SQLException e) {
                exceptionCollector.collectTerminateByServer(e);
                exceptionCollector.collectReadPrivileges("batchReadWithoutOffset", Collections.emptyList(), e);
                exceptionCollector.revealException(e);
                throw e;
            }
            if (EmptyKit.isNotEmpty(filterResults.getResults())) {
                consumer.accept(filterResults);
            }
        });
    }

    //for oracle db2 type (with row_number)
    protected void queryByAdvanceFilterWithOffsetV2(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        String sql = commonSqlMaker.buildSelectClause(table, filter) + commonSqlMaker.buildRowNumberPreClause(filter) + getSchemaAndTable(table.getId()) + commonSqlMaker.buildSqlByAdvanceFilterV2(filter);
        jdbcContext.query(sql, resultSet -> {
            FilterResults filterResults = new FilterResults();
            try {
                while (resultSet.next()) {
                    List<String> allColumn = DbKit.getColumnsFromResultSet(resultSet);
                    allColumn.remove("ROWNO_");
                    filterResults.add(DbKit.getRowFromResultSet(resultSet, allColumn));
                    if (filterResults.getResults().size() == BATCH_ADVANCE_READ_LIMIT) {
                        consumer.accept(filterResults);
                        filterResults = new FilterResults();
                    }
                }
            } catch (SQLException e) {
                exceptionCollector.collectTerminateByServer(e);
                exceptionCollector.collectReadPrivileges("batchReadWithoutOffset", Collections.emptyList(), e);
                exceptionCollector.revealException(e);
                throw e;
            }
            if (EmptyKit.isNotEmpty(filterResults.getResults())) {
                consumer.accept(filterResults);
            }
        });
    }

    protected void beginTransaction(TapConnectorContext connectorContext) throws Throwable {
        isTransaction = true;
    }

    protected void commitTransaction(TapConnectorContext connectorContext) throws Throwable {
        for (Map.Entry<String, Connection> entry : transactionConnectionMap.entrySet()) {
            try {
                entry.getValue().commit();
            } finally {
                EmptyKit.closeQuietly(entry.getValue());
            }
        }
        transactionConnectionMap.clear();
        isTransaction = false;
    }

    protected void rollbackTransaction(TapConnectorContext connectorContext) throws Throwable {
        for (Map.Entry<String, Connection> entry : transactionConnectionMap.entrySet()) {
            try {
                entry.getValue().rollback();
            } finally {
                EmptyKit.closeQuietly(entry.getValue());
            }
        }
        transactionConnectionMap.clear();
        isTransaction = false;
    }

    protected void queryIndexes(TapConnectorContext connectorContext, TapTable table, Consumer<List<TapIndex>> consumer) {
        consumer.accept(discoverIndex(table.getId()));
    }

    protected void dropIndexes(TapConnectorContext connectorContext, TapTable table, TapDeleteIndexEvent deleteIndexEvent) throws SQLException {
        char escapeChar = commonDbConfig.getEscapeChar();
        List<String> dropIndexesSql = new ArrayList<>();
        deleteIndexEvent.getIndexNames().forEach(idx -> dropIndexesSql.add("drop index " + getSchemaAndTable(table.getId()) + "." + escapeChar + idx + escapeChar));
        jdbcContext.batchExecute(dropIndexesSql);
    }

    protected long countRawCommand(TapConnectorContext connectorContext, String command, TapTable tapTable) throws SQLException {
        AtomicLong count = new AtomicLong(0);
        if (EmptyKit.isNotBlank(command) && command.trim().toLowerCase().startsWith("select")) {
            jdbcContext.query("select count(1) from (" + command + ") as tmp", resultSet -> {
                if (resultSet.next()) {
                    count.set(resultSet.getLong(1));
                }
            });
        }
        return count.get();
    }

    protected long countByAdvanceFilter(TapConnectorContext connectorContext, TapTable tapTable, TapAdvanceFilter tapAdvanceFilter) throws SQLException {
        AtomicLong count = new AtomicLong(0);
        String sql = "SELECT COUNT(1) FROM " + getSchemaAndTable(tapTable.getId()) + commonSqlMaker.buildSqlByAdvanceFilter(tapAdvanceFilter);
        jdbcContext.query(sql, resultSet -> {
            if (resultSet.next()) {
                count.set(resultSet.getLong(1));
            }
        });
        return count.get();
    }

    protected long countByAdvanceFilterV2(TapConnectorContext connectorContext, TapTable tapTable, TapAdvanceFilter tapAdvanceFilter) throws SQLException {
        AtomicLong count = new AtomicLong(0);
        String sql = "SELECT COUNT(1) FROM " + commonSqlMaker.buildRowNumberPreClause(tapAdvanceFilter) + getSchemaAndTable(tapTable.getId()) + commonSqlMaker.buildSqlByAdvanceFilterV2(tapAdvanceFilter);
        jdbcContext.query(sql, resultSet -> {
            if (resultSet.next()) {
                count.set(resultSet.getLong(1));
            }
        });
        return count.get();
    }
}
