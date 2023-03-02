package io.tapdata.common;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public abstract class CommonDbConnector extends ConnectorBase {

    private final static String selectPattern = "select \"%s\" from \"%s\".\"%s\" ";
    private final static String wherePattern = "where %s ";
    private final static Long offsetSize = 1000000L;

    protected JdbcContext jdbcContext;
    protected CommonDbConfig commonDbConfig;

    protected void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        List<String> primaryKeys = new ArrayList<>(tapTable.primaryKeys());
        String selectClause = String.format(selectPattern, String.join("\",\"", tapTable.getNameFieldMap().keySet()), commonDbConfig.getSchema(), tapTable.getId());
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
                        whereAppender.append(primaryKeys.stream().limit(i).map(col -> "\"" + col + "\"=?").collect(Collectors.joining(" and ")));
                        if (i > 0) {
                            whereAppender.append(" and ");
                        }
                        if (i == primaryKeys.size() - 1) {
                            whereAppender.append("\"").append(primaryKeys.get(i)).append("\"<=?");
                        } else {
                            whereAppender.append("\"").append(primaryKeys.get(i)).append("\"<?");
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
                        whereAppender.append(primaryKeys.stream().limit(i).map(col -> "\"" + col + "\"=?").collect(Collectors.joining(" and ")));
                        if (i > 0) {
                            whereAppender.append(" and ");
                        }
                        whereAppender.append("\"").append(primaryKeys.get(i)).append("\">?");
                        List<Object> params = primaryKeys.stream().limit(i + 1).map(from::get).collect(Collectors.toList());
                        submitOffsetReadEvents(selectClause + String.format(wherePattern, whereAppender), params, tapTable, eventBatchSize, eventsOffsetConsumer, offset);
                    }
                    StringBuilder whereAppenderMajor = new StringBuilder();
                    whereAppenderMajor.append(primaryKeys.stream().limit(sameKeySize).map(col -> "\"" + col + "\"=?").collect(Collectors.joining(" and ")));
                    if (sameKeySize > 0) {
                        whereAppenderMajor.append(" and ");
                    }
                    whereAppenderMajor.append("\"").append(primaryKeys.get(sameKeySize)).append("\">? and ").append("\"").append(primaryKeys.get(sameKeySize)).append("\"<?");
                    List<Object> paramsMajor = primaryKeys.stream().limit(sameKeySize + 1).map(from::get).collect(Collectors.toList());
                    paramsMajor.add(to.get(primaryKeys.get(sameKeySize)));
                    submitOffsetReadEvents(selectClause + String.format(wherePattern, whereAppenderMajor), paramsMajor, tapTable, eventBatchSize, eventsOffsetConsumer, offset);
                    for (int i = sameKeySize + 1; i <= primaryKeys.size(); i++) {
                        StringBuilder whereAppender = new StringBuilder();
                        whereAppender.append(primaryKeys.stream().limit(i).map(col -> "\"" + col + "\"=?").collect(Collectors.joining(" and ")));
                        if (i < primaryKeys.size()) {
                            whereAppender.append(" and ").append("\"").append(primaryKeys.get(i)).append("\"<?");
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
                        whereAppender.append(primaryKeys.stream().limit(i).map(col -> "\"" + col + "\"=?").collect(Collectors.joining(" and ")));
                        if (i > 0) {
                            whereAppender.append(" and ");
                        }
                        whereAppender.append("\"").append(primaryKeys.get(i)).append("\">?");
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
        while (isAlive() && resultSet.next()) {
            DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
            tapEvents.add(insertRecordEvent(dataMap, tapTable.getId()));
            if (tapEvents.size() == eventBatchSize) {
                eventsOffsetConsumer.accept(tapEvents, offset);
                tapEvents = list();
            }
        }
        //last events those less than eventBatchSize
        if (EmptyKit.isNotEmpty(tapEvents)) {
            eventsOffsetConsumer.accept(tapEvents, offset);
        }
    }

    private static final String FIND_KEY_FROM_OFFSET = "select * from (select \"%s\", row_number() over (order by \"%s\") as rowno from \"%s\".\"%s\" ) where rowno=%s";

    protected DataMap findPrimaryKeyValue(TapTable tapTable, Long offsetSize) throws Throwable {
        String primaryKeyString = String.join("\",\"", tapTable.primaryKeys());
        DataMap dataMap = new DataMap();
        jdbcContext.queryWithNext(String.format(FIND_KEY_FROM_OFFSET, primaryKeyString, primaryKeyString, commonDbConfig.getSchema(), tapTable.getId(), offsetSize),
                resultSet -> dataMap.putAll(DataMap.create(DbKit.getRowFromResultSet(resultSet, DbKit.getColumnsFromResultSet(resultSet)))));
        return dataMap;
    }
}
