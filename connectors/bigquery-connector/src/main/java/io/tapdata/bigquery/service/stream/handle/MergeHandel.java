package io.tapdata.bigquery.service.stream.handle;

import io.tapdata.bigquery.entity.ContextConfig;
import io.tapdata.bigquery.service.bigQuery.BigQueryResult;
import io.tapdata.bigquery.service.bigQuery.BigQueryStart;
import io.tapdata.bigquery.service.bigQuery.TableCreate;
import io.tapdata.bigquery.util.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

public class MergeHandel extends BigQueryStart {
    private static final String TAG = MergeHandel.class.getSimpleName();

    public static final String MERGE_KEY_ID = "merge_id";
    public static final String MERGE_KEY_ID_LAST = "merge_id_last";
    public static final String MERGE_KEY_TYPE = "merge_type";
    public static final String MERGE_KEY_DATA_BEFORE = "merge_data_before";
    public static final String MERGE_KEY_DATA_AFTER = "merge_data_after";
    public static final String MERGE_KEY_EVENT_TIME = "record_event_time";
    public static final String MERGE_KEY_TABLE_ID = "record_table_id";

    public static final String MERGE_VALUE_TYPE_INSERT = "I";
    public static final String MERGE_VALUE_TYPE_UPDATE = "U";
    public static final String MERGE_VALUE_TYPE_DELETE = "D";

    public static final String DELIMITER = ",";

    private static final String CLEAN_TABLE_SQL = "DELETE FROM `%s`.`%s`.`%s` WHERE " + MergeHandel.MERGE_KEY_ID + " <= %s;";
    private final static String DROP_TABLE_SQL = "DROP TABLE IF EXISTS `%s`.`%s`.`%s`;";
    public static final String MERGE_SQL =
            " MERGE `%s` merge_tab USING( " +
                    "SELECT * FROM `%s` targeted WHERE targeted." + MergeHandel.MERGE_KEY_ID + "<=%s AND targeted." + MergeHandel.MERGE_KEY_ID + ">%s" +
                    //"    SELECT * EXCEPT(row_num) FROM ( " +
                    //"        SELECT *, ROW_NUMBER() OVER(PARTITION BY delta."+MERGE_KEY_TYPE+" ORDER BY delta."+MERGE_KEY_ID+" DESC) AS row_num " +
                    //"        FROM `%s` delta " +
                    //"    ) " +
                    //"    WHERE row_num = 1 " +
                    " ) tab ON %s " + //merge_tab.id = tab.id
                    " WHEN NOT MATCHED AND tab." + MergeHandel.MERGE_KEY_TYPE + " in(\"" + MergeHandel.MERGE_VALUE_TYPE_INSERT + "\",\"" + MergeHandel.MERGE_VALUE_TYPE_UPDATE + "\") THEN " +
                    "   INSERT (%s) VALUES (%s) " + //id ,username, change_id       tab.id,tab.username,tab.change_id
                    " WHEN MATCHED AND tab." + MergeHandel.MERGE_KEY_TYPE + " = \"" + MergeHandel.MERGE_VALUE_TYPE_DELETE + "\" THEN " +
                    "   DELETE " +
                    " WHEN MATCHED AND tab." + MergeHandel.MERGE_KEY_TYPE + " = \"" + MergeHandel.MERGE_VALUE_TYPE_UPDATE + "\" AND tab." + MergeHandel.MERGE_KEY_ID + " <= %s AND tab." + MergeHandel.MERGE_KEY_ID + " > %s THEN " + //
                    "   UPDATE SET %s "; //username = tab.username, change_id = tab.change_id

    private final Object mergeLock = new Object();
    private long mergeDelaySeconds = ContextConfig.MERGE_DELAY_DEFAULT;
    private AtomicBoolean running = new AtomicBoolean(false);

    private ScheduledFuture<?> future;

    private TapTable mainTable;

    public MergeHandel mainTable(TapTable mainTable) {
        this.mainTable = mainTable;
        return this;
    }

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public MergeHandel running(AtomicBoolean run) {
        this.running = run;
        return this;
    }

    public MergeHandel mergeDelaySeconds(Long mergeDelaySeconds) {
        if (Objects.nonNull(mergeDelaySeconds)) {
            this.mergeDelaySeconds = mergeDelaySeconds;
        }
        return this;
    }

    public Object mergeLock() {
        return this.mergeLock;
    }

    public void stop() {
        if (Objects.nonNull(future)) {
            future.cancel(true);
            this.mergeTableOnce();
        }
    }

    private MergeHandel(TapConnectionContext connectorContext) {
        super(connectorContext);
    }

    public static MergeHandel merge(TapConnectionContext connectorContext) {
        return new MergeHandel(connectorContext);
    }

    /**
     * 创建临时表
     */
    public TapTable createTemporaryTable(TapTable table, String tableId) {
        TableCreate tableCreate = (TableCreate) TableCreate.create(this.connectorContext).paperStart(this);
        TapTable temporaryTable = table(tableId)
                .add(field(MergeHandel.MERGE_KEY_ID, JAVA_Long).isPrimaryKey(true).primaryKeyPos(1).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field(MergeHandel.MERGE_KEY_TYPE, JAVA_String).tapType(tapString().bytes(10L)))
                .add(field(MergeHandel.MERGE_KEY_DATA_BEFORE, JAVA_Map).tapType(tapMap()))
                .add(field(MergeHandel.MERGE_KEY_DATA_AFTER, JAVA_Map).tapType(tapMap()))
                .add(field(MergeHandel.MERGE_KEY_EVENT_TIME, JAVA_Long).isPrimaryKey(true).primaryKeyPos(1).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field(MergeHandel.MERGE_KEY_TABLE_ID, JAVA_String).tapType(tapString().bytes(1024L)));
        TapCreateTableEvent event = new TapCreateTableEvent();
        event.setTableId(tableId);
        event.setTable(temporaryTable);
        event.setReferenceTime(System.currentTimeMillis());
        if (tableCreate.isExist(event)) {
            TapLogger.info(TAG, "Temporary table [" + super.config().tempCursorSchema() + "] already exists.");
            return table;
        }
        this.createSchema(table, tableId);
        return table;
    }

    /**
     * x INT64 OPTIONS(description="An optional INTEGER field"),
     * y STRUCT<
     * a ARRAY<STRING> OPTIONS(description="A repeated STRING field"),
     * b BOOL
     * >
     */
    private void createSchema(TapTable table, String tableId) {
        if (Checker.isEmpty(this.config)) {
            throw new CoreException("Connection config is null or empty.");
        }
        String tableSet = this.config.tableSet();
        if (Checker.isEmpty(tableSet)) {
            throw new CoreException("Table set is null or empty.");
        }
        if (Checker.isEmpty(table)) {
            throw new CoreException("Tap table is null or empty.");
        }
        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        if (io.tapdata.bigquery.util.tool.Checker.isEmptyCollection(nameFieldMap)) {
            throw new CoreException("Tap table schema null or empty.");
        }
        String tableSetName = "`" + tableSet + "`.`" + tableId + "`";

        StringBuilder sql = new StringBuilder();//" DROP TABLE IF EXISTS ")
        sql.append("CREATE TABLE")
                .append(tableSetName)
                .append(" (")
                .append(MergeHandel.MERGE_KEY_ID).append(" INT64 OPTIONS(description=\"An optional INTEGER field\"),")
                .append(MergeHandel.MERGE_KEY_EVENT_TIME).append(" INT64 OPTIONS(description=\"An optional INTEGER field\"),")
                .append(MergeHandel.MERGE_KEY_TABLE_ID).append(" STRING OPTIONS(description=\"An optional INTEGER field\"),")
                .append(MergeHandel.MERGE_KEY_TYPE).append(" STRING OPTIONS(description=\"I/U/D, is TapEventType\"),");
        StringBuilder structSql = new StringBuilder();
        nameFieldMap.forEach((key, tapField) -> {
            String dataType = tapField.getDataType();
            structSql.append(" `")
                    .append(key)
                    .append("` ")
                    .append(dataType.toUpperCase());
            //DEFAULT
            String defaultValue = tapField.getDefaultValue() == null ? "" : tapField.getDefaultValue().toString();
            if (io.tapdata.bigquery.util.tool.Checker.isNotEmpty(defaultValue)) {
                if (defaultValue.contains("'")) {
                    defaultValue = defaultValue.replaceAll("'", "\\'");
                }
                if (tapField.getTapType() instanceof TapNumber) {
                    defaultValue = defaultValue.trim();
                }
                structSql.append(" DEFAULT '").append(defaultValue).append("' ");
            }

            // comment
            String comment = tapField.getComment();
            structSql.append(" OPTIONS (");
            if (io.tapdata.bigquery.util.tool.Checker.isNotEmpty(comment)) {
                comment = comment.replaceAll("'", "\\'");
                structSql.append(" description = '").append(comment).append("' ");
            }
            //if has next option please split by comment [,]
            structSql.append(" ),");
        });
        if (structSql.lastIndexOf(",") == structSql.length() - 1) {
            structSql.deleteCharAt(structSql.lastIndexOf(","));
        }
        sql.append(MergeHandel.MERGE_KEY_DATA_BEFORE).append(" STRUCT<").append(structSql).append(">,")
                .append(MergeHandel.MERGE_KEY_DATA_AFTER).append(" STRUCT<").append(structSql).append(">");
        String comment = table.getComment();
        sql.append(" ) ");
        String collateSpecification = "";//默认排序规则
        if (io.tapdata.bigquery.util.tool.Checker.isNotEmpty(collateSpecification)) {
            sql.append(" DEFAULT COLLATE ").append(collateSpecification).append(" ");
        }
        sql.append(" OPTIONS ( ");
        if (io.tapdata.bigquery.util.tool.Checker.isNotEmpty(comment)) {
            comment = comment.replaceAll("'", "\\'");
            sql.append(" description = '").append(comment).append("' ");
        }
        sql.append(" );");
        BigQueryResult bigQueryResult = super.sqlMarker.executeOnce(sql.toString());
        if (Checker.isEmpty(bigQueryResult)) {
            throw new CoreException("Create table error.");
        }
    }

    public List<TapRecordEvent> temporaryEvent(List<TapRecordEvent> eventList) {
        List<TapRecordEvent> insertRecordEvent = new ArrayList<>();
        for (TapRecordEvent event : eventList) {
            TapInsertRecordEvent insert;
            Map<String, Object> after = new HashMap<>();
            if (event instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent recordEvent = (TapInsertRecordEvent) event;
                after.put(MergeHandel.MERGE_KEY_TYPE, MergeHandel.MERGE_VALUE_TYPE_INSERT);
                after.put(MergeHandel.MERGE_KEY_DATA_AFTER, recordEvent.getAfter());
                after.put(MergeHandel.MERGE_KEY_DATA_BEFORE, recordEvent.getAfter());
            } else if (event instanceof TapUpdateRecordEvent) {
                TapUpdateRecordEvent recordEvent = (TapUpdateRecordEvent) event;
                after.put(MergeHandel.MERGE_KEY_TYPE, MergeHandel.MERGE_VALUE_TYPE_UPDATE);
                after.put(MergeHandel.MERGE_KEY_DATA_BEFORE, recordEvent.getBefore());
                after.put(MergeHandel.MERGE_KEY_DATA_AFTER, recordEvent.getAfter());
            } else if (event instanceof TapDeleteRecordEvent) {
                TapDeleteRecordEvent recordEvent = (TapDeleteRecordEvent) event;
                after.put(MergeHandel.MERGE_KEY_TYPE, MergeHandel.MERGE_VALUE_TYPE_DELETE);
                after.put(MergeHandel.MERGE_KEY_DATA_BEFORE, recordEvent.getBefore());
            } else {
                continue;
            }
            long nanoTime = System.nanoTime();
            after.put(MergeHandel.MERGE_KEY_ID, nanoTime);
            after.put(MergeHandel.MERGE_KEY_EVENT_TIME, event.getReferenceTime());
            after.put(MergeHandel.MERGE_KEY_TABLE_ID, event.getTableId());
            insert = new TapInsertRecordEvent();
            insert.after(after);
            insert.referenceTime(event.getReferenceTime());
            insert.setTableId(super.config().tempCursorSchema());
            insertRecordEvent.add(insert);
        }
        return insertRecordEvent;
    }

    /**
     * 清空临时表
     */
    public void cleanTemporaryTable() {
        if (Checker.isEmpty(super.config())) {
            throw new CoreException("Connection config cannot be empty.");
        }
        String projectId = super.config().projectId();
        if (Checker.isEmpty(projectId)) {
            throw new CoreException("Project ID cannot be empty.");
        }
        String tableSet = super.config().tableSet();
        if (Checker.isEmpty(tableSet)) {
            throw new CoreException("Table set cannot not be empty.");
        }
        if (Checker.isEmpty(super.config().tempCursorSchema())) {
            throw new CoreException("Tap table cannot be empty.");
        }
        KVMap<Object> stateMap = ((TapConnectorContext) this.connectorContext).getStateMap();
        Object newestRecordId = stateMap.get(MergeHandel.MERGE_KEY_ID);
        if (Objects.isNull(newestRecordId)) {
            newestRecordId = 0;
        }
        try {
            super.sqlMarker.executeOnce(String.format(
                    MergeHandel.CLEAN_TABLE_SQL,
                    projectId,
                    tableSet,
                    super.config().tempCursorSchema(),
                    newestRecordId));
        } catch (Exception e) {
            throw new CoreException("Failed to empty temporary table, table name is " + super.config().tempCursorSchema() + ", " + e.getMessage());
        }
    }

    public boolean dropTemporaryTable() {
        return this.dropTemporaryTable(super.config().tempCursorSchema());
    }

    public boolean dropTemporaryTable(String temporaryTableId) {
        if (Checker.isEmpty(temporaryTableId)) {
            throw new CoreException("Drop event error,table name cannot be empty.");
        }
        try {
            BigQueryResult bigQueryResult = super.sqlMarker.executeOnce(
                    String.format(MergeHandel.DROP_TABLE_SQL
                            , super.config().projectId()
                            , super.config().tableSet()
                            , temporaryTableId));
            return Checker.isNotEmpty(bigQueryResult) && Checker.isNotEmpty(bigQueryResult.getTotalRows()) && bigQueryResult.getTotalRows() > 0;
        } catch (Exception e) {
            throw new CoreException(String.format("Drop temporary table error,table name is: %s, error is: %s.", temporaryTableId, e.getMessage()));
        }
    }

    /**
     * 合并零时表到主表 - 混合模式
     */
    public void mergeTemporaryTableToMainTable(TapTable mainTable) {
        if (Objects.isNull(mainTable)) {
            throw new CoreException("TableTable must not be null or not be empty.");
        }
        this.mainTable = mainTable;
        if (Objects.isNull(this.future)) {
            this.future = this.scheduledExecutorService.scheduleWithFixedDelay(() -> {
                synchronized (this.mergeLock) {
                    try {
                        this.mergeTableOnce();
                    } catch (Throwable throwable) {
                        TapLogger.error(TAG, "Try upload failed in scheduler, {}", throwable.getMessage());
                    }
                }
            }, 60, this.mergeDelaySeconds, TimeUnit.SECONDS);
        }
    }

    public void mergeTableOnce() {
        KVMap<Object> stateMap = ((TapConnectorContext) this.connectorContext).getStateMap();
        Object mergeKeyId = stateMap.get(MergeHandel.MERGE_KEY_ID);
        Object mergeKeyIdLast = stateMap.get(MergeHandel.MERGE_KEY_ID_LAST);
        if (Objects.isNull(mergeKeyIdLast)) {
            mergeKeyIdLast = 0L;
        }
        SQLBuilder builder = this.mergeSql(this.mainTable);
        String projectAndSetId = super.config().projectId() + "." + super.config().tableSet() + ".";
        Object finalMergeKeyId = Objects.isNull(mergeKeyId) ? 0 : mergeKeyId;
        try {
            if (Long.parseLong(String.valueOf(finalMergeKeyId)) < Long.parseLong(String.valueOf(mergeKeyIdLast))) {
                mergeKeyIdLast = 0L;
            }
        } catch (Exception e) {
            mergeKeyIdLast = 0L;
        }
        String sql = String.format(
                MergeHandel.MERGE_SQL,
                projectAndSetId + this.mainTable.getId(),
                projectAndSetId + super.config().tempCursorSchema(),
                finalMergeKeyId,
                mergeKeyIdLast,
                builder.whereSql(),
                builder.insertKeySql(),
                builder.insertValueSql(),
                finalMergeKeyId,
                mergeKeyIdLast,
                builder.updateSql());
        super.sqlMarker.executeOnce(sql);
        stateMap.put(MergeHandel.MERGE_KEY_ID_LAST, finalMergeKeyId);
        this.cleanTemporaryTable();
    }

    public SQLBuilder mergeSql(TapTable table) {
        if (Objects.isNull(table)) {
            throw new CoreException("TapTable must not be null or not be empty.");
        }
        Map<String, TapField> fieldMap = table.getNameFieldMap();
        if (Objects.isNull(fieldMap) || fieldMap.isEmpty()) {
            throw new CoreException("TapTable's field map must not be null or not be empty, table name is " + table.getId() + ".");
        }
        Collection<String> primaryKeys = table.primaryKeys(true);
        boolean hasPrimaryKeys = Objects.nonNull(primaryKeys) && !primaryKeys.isEmpty();
        StringJoiner whereSql = new StringJoiner(" AND ");
        StringJoiner insertKeySql = new StringJoiner(MergeHandel.DELIMITER);
        StringJoiner insertValueSql = new StringJoiner(MergeHandel.DELIMITER);
        StringJoiner updateSql = new StringJoiner(MergeHandel.DELIMITER);
        fieldMap.forEach((key, field) -> {
            insertKeySql.add(key);
            insertValueSql.add("tab." + MergeHandel.MERGE_KEY_DATA_AFTER + "." + key);
            updateSql.add(key + " = tab." + MergeHandel.MERGE_KEY_DATA_AFTER + "." + key);
            if (!hasPrimaryKeys) {
                whereSql.add("tab." + MergeHandel.MERGE_KEY_DATA_BEFORE + "." + key + " = merge_tab." + key);
            }
        });
        if (hasPrimaryKeys) {
            primaryKeys.stream().filter(Objects::nonNull).forEach(key -> {
                whereSql.add("tab." + MergeHandel.MERGE_KEY_DATA_BEFORE + "." + key + " = merge_tab." + key);
            });
        }
        return new SQLBuilder(
                whereSql.toString(),
                insertKeySql.toString(),
                insertValueSql.toString(),
                updateSql.toString());
    }

    static class SQLBuilder {
        String whereSql;
        String insertKeySql;
        String insertValueSql;
        String updateSql;

        public String whereSql() {
            return this.whereSql;
        }

        public String insertKeySql() {
            return this.insertKeySql;
        }

        public String insertValueSql() {
            return this.insertValueSql;
        }

        public String updateSql() {
            return this.updateSql;
        }

        public SQLBuilder(String whereSql, String insertKeySql, String insertValueSql, String updateSql) {
            this.whereSql = whereSql;
            this.insertKeySql = insertKeySql;
            this.updateSql = updateSql;
            this.insertValueSql = insertValueSql;
        }
    }
}
