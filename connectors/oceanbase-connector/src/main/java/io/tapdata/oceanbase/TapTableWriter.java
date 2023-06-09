package io.tapdata.oceanbase;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.oceanbase.connector.OceanbaseConnector;
import io.tapdata.oceanbase.dml.IWriter;
import io.tapdata.oceanbase.util.JdbcUtil;
import io.tapdata.oceanbase.util.LRUOnRemoveMap;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/9/20 16:42 Create
 */
public class TapTableWriter implements IWriter<TapRecordEvent, WriteListResult<TapRecordEvent>> {

    private final Connection connection;
    private final TapTable tapTable;
    private final Supplier<Boolean> isRunning;

    private final LinkedHashSet<String> uniqueCondition;
    private final String insertPolicy;
    private final String updatePolicy;

    protected final Map<String, PreparedStatement> statementMap = new LRUOnRemoveMap<>(10, s -> JdbcUtil.closeQuietly(s.getValue()));
    protected String lastStatementKey;
    protected Type lastStatementType;
    protected PreparedStatement lastStatement;
    protected int batchCounts = 0;
    protected int batchLimit = 5;

    public TapTableWriter(Connection connection, TapTable tapTable, Supplier<Boolean> isRunning, String insertPolicy, String updatePolicy) {
        this.connection = connection;
        this.tapTable = tapTable;
        this.isRunning = isRunning;
        this.insertPolicy = insertPolicy;
        this.updatePolicy = updatePolicy;
        this.uniqueCondition = new LinkedHashSet<>(tapTable.primaryKeys(false));
    }

    @Override
    public void addBath(TapRecordEvent recordEvent, WriteListResult<TapRecordEvent> writeListResult) throws Exception {
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
                lastStatement = getInsertStatement(statementKey, (TapInsertRecordEvent) recordEvent);
                doInsert((TapInsertRecordEvent) recordEvent);
                break;
            }
            case Update: {
                TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                if (uniqueCondition.isEmpty()) {
                    throw new RuntimeException("DML update operations without associated conditions are not supported");
                } else if (null == updateRecordEvent.getAfter()) {
                    throw new RuntimeException("Record event after data is null: " + updateRecordEvent);
                } else if (updateRecordEvent.getAfter().isEmpty()) {
                    throw new RuntimeException("Record event after data is empty: " + updateRecordEvent);
                }
                lastStatement = getUpdateStatement(statementKey, updateRecordEvent);
                doUpdate(updateRecordEvent);
                break;
            }
            case Delete: {
                TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                if (uniqueCondition.isEmpty()) {
                    throw new RuntimeException("DML delete operations without associated conditions are not supported");
                } else if (null == deleteRecordEvent.getBefore()) {
                    throw new RuntimeException("Record event before data is null: " + deleteRecordEvent);
                } else if (deleteRecordEvent.getBefore().isEmpty()) {
                    throw new RuntimeException("Record event before data is empty: " + deleteRecordEvent);
                }
                lastStatement = getDeleteStatement(statementKey);
                doDelete((TapDeleteRecordEvent) recordEvent);
                summit(writeListResult);
                break;
            }
            default:
                throw new RuntimeException("not support type: " + type);
        }
    }

    @Override
    public void summit(WriteListResult<TapRecordEvent> result) throws Exception {
        if (null != lastStatementType && null != lastStatementKey && null != lastStatement) {
            for (int i = 1; i < 4 && isRunning.get(); i++) {
                try {
                    lastStatement.executeBatch();
                    lastStatement.clearParameters();

                    Connection connection = lastStatement.getConnection();
                    if (!connection.getAutoCommit()) {
                        connection.commit();
                    }
                } catch (SQLException e) {
                    if (4030 == e.getErrorCode() && "HY000".equals(e.getSQLState())) {
                        TapLogger.warn(OceanbaseConnector.TAG, e.getMessage() + " with retry(" + i + ") after 5 seconds");
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
        return type + "-" + String.join(",", data.keySet());
    }

    protected void doInsert(TapInsertRecordEvent event) throws Exception {
        int i = 1;
        for (Map.Entry<String, Object> en : event.getAfter().entrySet()) {
            lastStatement.setObject(i, en.getValue());
            i++;
        }
        lastStatement.addBatch();
    }

    protected void doUpdate(TapUpdateRecordEvent event) throws Exception {
        // Not all sources can provide Before data and need to be compatible
        Map<String, Object> beforeData = event.getBefore();
        if (null == beforeData || beforeData.isEmpty()) {
            beforeData = event.getAfter();
        }

        int i = 1;
        if (ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS.equals(updatePolicy)) {
            for (String field : uniqueCondition) {
                lastStatement.setObject(i, beforeData.get(field));
                i++;
            }
            for (Map.Entry<String, Object> en : event.getAfter().entrySet()) {
                if (uniqueCondition.contains(en.getKey())) {
                    continue;
                }
                lastStatement.setObject(i, en.getValue());
                i++;
            }
        } else {
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
        }
        lastStatement.addBatch();
    }

    protected void doDelete(TapDeleteRecordEvent event) throws Exception {
        int i = 1;
        Map<String, Object> data = event.getBefore();
        for (String field : uniqueCondition) {
            lastStatement.setObject(i, data.get(field));
            i++;
        }
        lastStatement.addBatch();
    }

    protected PreparedStatement getInsertStatement(String statementKey, TapInsertRecordEvent recordEvent) throws Exception {
        return statementMap.computeIfAbsent(statementKey, k -> {
            try {
                StringBuilder sql = new StringBuilder("insert");
                if (uniqueCondition.isEmpty()) sql.append(" ignore");
                sql.append(" into `").append(tapTable.getId()).append("`(");

                Set<String> fields = recordEvent.getAfter().keySet();
                for (String field : fields) sql.append("`").append(field).append("`,");
                sql.setLength(sql.length() - 1);
                sql.append(")");

                sql.append(" values(");
                for (String ignore : fields) sql.append("?,");
                sql.setLength(sql.length() - 1);
                sql.append(")");

                if (!uniqueCondition.isEmpty() && ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS.equals(insertPolicy)) {
                    sql.append(" ON DUPLICATE KEY UPDATE ");
                    for (String field : fields) {
                        if (uniqueCondition.contains(field)) continue; // 忽略主键
                        sql.append("`").append(field).append("`=values(`").append(field).append("`),");
                    }
                    sql.setLength(sql.length() - 1);
                }

                TapLogger.info(OceanbaseConnector.TAG, "insert sql: " + sql);
                return connection.prepareStatement(sql.toString());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected PreparedStatement getUpdateStatement(String statementKey, TapUpdateRecordEvent recordEvent) {
        // Not support change condition keys
        Object afterValue, beforeValue;
        Map<String, Object> afterData = recordEvent.getAfter();
        Map<String, Object> beforeData = recordEvent.getBefore();

        // Not all sources can provide Before data and need to be compatible
        if (null == beforeData || beforeData.isEmpty()) {
            beforeData = recordEvent.getAfter();
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
            throw new RuntimeException("Not support change condition keys: " + recordEvent);
        }

        return statementMap.computeIfAbsent(statementKey, k -> {
            try {
                StringBuilder sql = new StringBuilder();
                if (ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS.equals(updatePolicy)) {
                    sql.append("insert into `").append(tapTable.getId()).append("`(");

                    int fieldCounts = 0;
                    for (String field : uniqueCondition) {
                        sql.append("`").append(field).append("`,");
                        fieldCounts++;
                    }
                    for (String field : afterData.keySet()) {
                        if (uniqueCondition.contains(field)) {
                            continue;
                        }
                        sql.append("`").append(field).append("`,");
                        fieldCounts++;
                    }
                    sql.setLength(sql.length() - 1);
                    sql.append(")");

                    sql.append(" values(");
                    for (; fieldCounts > 0; fieldCounts--) {
                        sql.append("?,");
                    }
                    sql.setLength(sql.length() - 1);
                    sql.append(")");

                    if (ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS.equals(insertPolicy)) {
                        sql.append(" ON DUPLICATE KEY UPDATE ");
                        for (String field : afterData.keySet()) {
                            if (uniqueCondition.contains(field)) continue; // 忽略主键
                            sql.append("`").append(field).append("`=values(`").append(field).append("`),");
                        }
                        sql.setLength(sql.length() - 1);
                    }
                } else {
                    sql.append("update `").append(tapTable.getId()).append("` set ");
                    for (String field : afterData.keySet()) {
                        if (uniqueCondition.contains(field)) continue;
                        sql.append("`").append(field).append("`=?,");
                    }
                    sql.setLength(sql.length() - 1);
                    sql.append(" where ");
                    for (String field : uniqueCondition) {
                        sql.append("`").append(field).append("`=? and");
                    }
                    sql.setLength(sql.length() - 4);
                }

                TapLogger.info(OceanbaseConnector.TAG, "update sql: " + sql);
                return connection.prepareStatement(sql.toString());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected PreparedStatement getDeleteStatement(String statementKey) {
        if (uniqueCondition.isEmpty())
            throw new RuntimeException("DML delete operations without associated conditions are not supported");
        return statementMap.computeIfAbsent(statementKey, k -> {
            try {
                StringBuilder sql = new StringBuilder("delete from");
                sql.append(" `").append(tapTable.getId()).append("` where ");
                for (String field : uniqueCondition) {
                    sql.append("`").append(field).append("`=? and");
                }
                sql.setLength(sql.length() - 4);
                TapLogger.info(OceanbaseConnector.TAG, "delete sql: " + sql);
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
}
