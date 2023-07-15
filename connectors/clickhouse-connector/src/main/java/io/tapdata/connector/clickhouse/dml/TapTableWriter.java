package io.tapdata.connector.clickhouse.dml;

import io.tapdata.connector.clickhouse.util.JdbcUtil;
import io.tapdata.connector.clickhouse.util.LRUOnRemoveMap;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;

import java.sql.*;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/9/20 16:42 Create
 */
public class TapTableWriter implements IWriter<TapRecordEvent, WriteListResult<TapRecordEvent>> {

    private final String connectorTag;
    private final Connection connection;
    private final String database;
    private final Supplier<Boolean> isRunning;

    private final String insertPolicy;
    private final String updatePolicy;

    protected final Map<String, PreparedStatement> statementMap = new LRUOnRemoveMap<>(10, s -> JdbcUtil.closeQuietly(s.getValue()));
    protected String lastStatementKey;
    protected Type lastStatementType;
    protected PreparedStatement existsStatement;
    protected PreparedStatement lastStatement;
    protected int batchCounts = 0;
    protected int batchLimit = 1000;
    protected boolean optimizeTable = true;

    public TapTableWriter(String connectorTag, Connection connection, String database, Supplier<Boolean> isRunning, String insertPolicy, String updatePolicy) {
        this.connectorTag = connectorTag;
        this.connection = connection;
        this.database = database;
        this.isRunning = isRunning;
        this.insertPolicy = insertPolicy;
        this.updatePolicy = updatePolicy;
    }

    public void optimizeTable(TapTable tapTable) throws SQLException {
        if (optimizeTable) {
            try (Statement s = connection.createStatement()) {
                s.execute("optimize table `" + tapTable.getId() + "` final");
                if (!connection.getAutoCommit()) {
                    connection.commit();
                }
            } catch (SQLException e) {
                TapLogger.warn(connectorTag, "optimize table({}) failed: {}", tapTable.getId(), e.getMessage(), e);
                optimizeTable = false;
            }
        }
    }

    @Override
    public void addBath(TapTable tapTable, TapRecordEvent recordEvent, WriteListResult<TapRecordEvent> writeListResult) throws Exception {
        Type type = Type.parse(recordEvent);
        String statementKey = statementKey(type, recordEvent);
        if (!statementKey.equals(lastStatementKey)) {
            summit(writeListResult);
            lastStatementKey = statementKey;
            lastStatementType = type;
        }

        batchCounts++;
        if (batchCounts >= batchLimit) {
            summit(writeListResult);
        }
        switch (type) {
            case Insert: {
                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                if (null == insertRecordEvent.getAfter()) {
                    throw new RuntimeException("Record event after data is null: " + insertRecordEvent);
                } else if (insertRecordEvent.getAfter().isEmpty()) {
                    throw new RuntimeException("Record event after data is empty: " + insertRecordEvent);
                }
                lastStatement = getInsertStatement(tapTable, statementKey, insertRecordEvent.getAfter());
                doInsert(insertRecordEvent.getAfter());
                break;
            }
            case Update: {
                TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                LinkedHashSet<String> uniqueCondition = new LinkedHashSet<>(tapTable.primaryKeys(true));
                if (uniqueCondition.isEmpty()) {
                    throw new RuntimeException("DML update operations without associated conditions are not supported");
                } else if (null == updateRecordEvent.getAfter()) {
                    throw new RuntimeException("Record event after data is null: " + updateRecordEvent);
                } else if (updateRecordEvent.getAfter().isEmpty()) {
                    throw new RuntimeException("Record event after data is empty: " + updateRecordEvent);
                }
                lastStatement = getUpdateStatement(tapTable, uniqueCondition, statementKey, updateRecordEvent.getBefore(), updateRecordEvent.getAfter());
                doUpdate(uniqueCondition, updateRecordEvent);
                break;
            }
            case Delete: {
                TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                LinkedHashSet<String> uniqueCondition = new LinkedHashSet<>(tapTable.primaryKeys(true));
                if (uniqueCondition.isEmpty()) {
                    throw new RuntimeException("DML delete operations without associated conditions are not supported");
                } else if (null == deleteRecordEvent.getBefore()) {
                    throw new RuntimeException("Record event before data is null: " + deleteRecordEvent);
                } else if (deleteRecordEvent.getBefore().isEmpty()) {
                    throw new RuntimeException("Record event before data is empty: " + deleteRecordEvent);
                }
                lastStatement = getDeleteStatement(tapTable, uniqueCondition, statementKey);
                doDelete(uniqueCondition, (TapDeleteRecordEvent) recordEvent);
                break;
            }
            default:
                throw new RuntimeException("not support type: " + type);
        }
    }

    @Override
    public void summit(WriteListResult<TapRecordEvent> result) throws Exception {
        if (batchCounts > 0 && null != lastStatementType && null != lastStatementKey && null != lastStatement) {
            for (int i = 1; i < 4 && isRunning.get(); i++) {
                try {
                    switch (lastStatementType) {
                        case Insert:
                            lastStatement.executeBatch();
                            break;
                        default:
                            break;
                    }

                    Connection connection = lastStatement.getConnection();
                    if (!connection.getAutoCommit()) {
                        connection.commit();
                    }
                    lastStatement.clearBatch();
                } catch (SQLException e) {
                    if (4030 == e.getErrorCode() && "HY000".equals(e.getSQLState())) {
                        TapLogger.warn(connectorTag, e.getMessage() + " with retry(" + i + ") after 5 seconds");
                        Thread.sleep(5000);
                        continue;
                    }
                    throw e;
                }
            }

            switch (lastStatementType) {
                case Insert:
                    result.incrementInserted(batchCounts);
                    break;
                case Update:
                    result.incrementModified(batchCounts);
                    break;
                case Delete:
                    result.incrementRemove(batchCounts);
                    break;
                default:
                    break;
            }
            batchCounts = 0;
        }
    }

    private boolean checkExists(TapTable tapTable, Map<String, Object> data) throws SQLException {
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        if (CollectionUtils.isEmpty(primaryKeys)) {
            return false;
        }

        if (null == existsStatement) {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT COUNT(1) AS COUNTS ").append(sqlQuota(".", database, tapTable.getId())).append(" WHERE");
            for (String fieldName : primaryKeys) {
                sql.append(" ").append(sqlQuota(fieldName)).append("<=>? AND");
            }
            sql.setLength(sql.length() - 4);
            existsStatement = connection.prepareStatement(sql.toString());
        }

        int i = 1;
        for (String fieldName : primaryKeys) {
            existsStatement.setObject(i++, data.get(fieldName));
        }
        try (ResultSet rs = existsStatement.executeQuery()) {
            if (rs.next()) {
                return rs.getInt("COUNTS") > 0;
            }
            return false;
        }
    }

    protected String statementKey(Type type, TapRecordEvent record) {
        Map<String, Object> data;
        switch (type) {
            case Insert:
                data = ((TapInsertRecordEvent) record).getAfter();
                break;
            case Update:
                data = ((TapUpdateRecordEvent) record).getAfter();
                break;
            case Delete:
                data = ((TapDeleteRecordEvent) record).getBefore();
                break;
            default:
                throw new RuntimeException("not support type: " + type);
        }
        return type + "-" + record.getTableId() + "-" + String.join(",", data.keySet());
    }

    protected void doInsert(Map<String, Object> afterData) throws Exception {
        int i = 1;
        for (Map.Entry<String, Object> en : afterData.entrySet()) {
            lastStatement.setObject(i, en.getValue());
            i++;
        }
        lastStatement.addBatch();
    }

    protected void doUpdate(LinkedHashSet<String> uniqueCondition, TapUpdateRecordEvent event) throws Exception {
        // Not all sources can provide Before data and need to be compatible
        Map<String, Object> beforeData = event.getBefore();
        if (null == beforeData || beforeData.isEmpty()) {
            beforeData = event.getAfter();
        }

        int i = 1;
        for (Map.Entry<String, Object> en : event.getAfter().entrySet()) {
            if (uniqueCondition.contains(en.getKey())) {
                continue;
            }
            lastStatement.setObject(i, en.getValue());
            i++;
        }
        for (String field : uniqueCondition) {
            lastStatement.setObject(i, beforeData.get(field));
            i++;
        }
        lastStatement.executeUpdate();
    }

    protected void doDelete(LinkedHashSet<String> uniqueCondition, TapDeleteRecordEvent event) throws Exception {
        int i = 1;
        Map<String, Object> data = event.getBefore();
        for (String field : uniqueCondition) {
            lastStatement.setObject(i, data.get(field));
            i++;
        }
        lastStatement.executeUpdate();
    }

    protected PreparedStatement getInsertStatement(TapTable tapTable, String statementKey, Map<String, Object> afterData) throws Exception {
        return statementMap.computeIfAbsent(statementKey, k -> {
            try {
                Set<String> fields = afterData.keySet();
                StringBuilder sql = new StringBuilder("INSERT");
                sql.append(" INTO ").append(sqlQuota(".", database, tapTable.getId())).append("(");

                for (String field : fields) sql.append(sqlQuota(field)).append(",");
                sql.setLength(sql.length() - 1);
                sql.append(")");

                sql.append(" VALUES(");
                for (String ignore : fields) sql.append("?,");
                sql.setLength(sql.length() - 1);
                sql.append(")");

//                TapLogger.info(connectorTag, "insert sql: " + sql);
                return connection.prepareStatement(sql.toString());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected PreparedStatement getUpdateStatement(TapTable tapTable, LinkedHashSet<String> uniqueCondition, String statementKey, Map<String, Object> beforeData, Map<String, Object> afterData) {
        // Not support change condition keys
        Object afterValue, beforeValue;

        // Not all sources can provide Before data and need to be compatible
        if (null == beforeData || beforeData.isEmpty()) {
            beforeData = afterData;
        }

        for (String field : uniqueCondition) {
            afterValue = afterData.get(field);
            beforeValue = beforeData.get(field);
            if (null == afterValue) {
                if (null == beforeValue) {
                    continue;
                }
            } else if (afterValue.equals(beforeValue)) {
                continue;
            }
            throw new RuntimeException("Not support change condition keys");
        }

        return statementMap.computeIfAbsent(statementKey, k -> {
            try {
                StringBuilder sql = new StringBuilder();
                sql.append("ALTER TABLE ").append(sqlQuota(".", database, tapTable.getId())).append(" UPDATE ");
                for (String field : afterData.keySet()) {
                    if (uniqueCondition.contains(field)) continue;
                    sql.append(sqlQuota(field)).append("=?,");
                }
                sql.setLength(sql.length() - 1);
                sql.append(" WHERE");
                for (String field : uniqueCondition) {
                    sql.append(" ").append(sqlQuota(field)).append("=? AND");
                }
                sql.setLength(sql.length() - 4);

//                TapLogger.info(connectorTag, "update sql: " + sql);
                return connection.prepareStatement(sql.toString());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected PreparedStatement getDeleteStatement(TapTable tapTable, LinkedHashSet<String> uniqueCondition, String statementKey) {
        return statementMap.computeIfAbsent(statementKey, k -> {
            if (uniqueCondition.isEmpty())
                throw new RuntimeException("DML delete operations without associated conditions are not supported");
            try {
                StringBuilder sql = new StringBuilder("ALTER TABLE ");
                sql.append(sqlQuota(".", database, tapTable.getId())).append(" DELETE WHERE");
                for (String field : uniqueCondition) {
                    sql.append(" ").append(sqlQuota(field)).append("=? AND");
                }
                sql.setLength(sql.length() - 4);
//                TapLogger.info(connectorTag, "delete sql: " + sql);
                return connection.prepareStatement(sql.toString());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void close() throws Exception {
        statementMap.clear();
    }

    private static final char QUOTA = '`';

    public static String sqlQuota(String name) {
        return QUOTA + name + QUOTA;
    }

    public static String sqlQuota(String delimiter, String... names) {
        return QUOTA + String.join(QUOTA + delimiter + QUOTA, names) + QUOTA;
    }

    public static String sqlQuota(String delimiter, Iterable<String> names) {
        return QUOTA + String.join(QUOTA + delimiter + QUOTA, names) + QUOTA;
    }
}
