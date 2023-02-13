package io.tapdata.bigquery.service.stream.v2;

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
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

public class MergeHandel extends BigQueryStart {
    private static final String TAG = MergeHandel.class.getSimpleName();
    public static final Long FIRST_MERGE_DELAY_SECOND = 33 * 60L;
    public static final Long DEFAULT_MERGE_DELAY_SECOND = 3600L;

    public static final String BATCH_TO_STREAM_TIME = "BATCH_TO_STREAM_TIME";
    public static final String MERGE_KEY_ID = "merge_id";
    public static final String MERGE_KEY_ID_LAST = "merge_id_last";
    public static final String HAS_MERGED = "has_merged";

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

    private final Map<String, TapTable> mergeTable = new ConcurrentHashMap<>();

    public MergeHandel needMerge(TapTable needMergeTable) {
        Optional.ofNullable(needMergeTable).ifPresent(tab -> this.mergeTable.put(needMergeTable.getId(), needMergeTable));
        return this;
    }

    /**
     * SELECT * EXCEPT(row_num) FROM (
     * SELECT *, ROW_NUMBER() OVER(PARTITION BY delta.merge_data_before.id ORDER BY delta.merge_id ASC) AS row_num
     * FROM `vibrant-castle-366614.SchemaoOfJoinSet.temp_BigData_cdba0c25_46f0_4061_8a6e_eea1a8e0205a` delta
     * )
     * WHERE row_num = 1
     * ) tab
     */
    public static final String MERGE_SQL =
            " MERGE `%s` merge_tab USING( " +  //target table id
                    "    SELECT * EXCEPT(row_num) FROM ( " +
                    "        SELECT *, ROW_NUMBER() OVER(PARTITION BY %s ORDER BY delta." + MERGE_KEY_ID + " DESC) AS row_num " + // Primary keys separated by commas (from delta.merge_data_before. in the assembly result)
                    "        FROM `%s` delta where delta." + MergeHandel.MERGE_KEY_ID + "<=%s AND delta." + MergeHandel.MERGE_KEY_ID + ">%s" + //source table id ;  Left boundary ；right boundary
                    "    ) " +
                    "    WHERE row_num = 1 " +
                    " ) tab ON %s " +  //匹配值
                    " WHEN NOT MATCHED BY TARGET THEN " +
                    "   INSERT (%s) VALUES (%s) " +
                    " WHEN MATCHED AND tab." + MergeHandel.MERGE_KEY_TYPE + " = \"" + MergeHandel.MERGE_VALUE_TYPE_DELETE + "\" THEN " +
                    "   DELETE " +
                    " WHEN MATCHED AND tab." + MergeHandel.MERGE_KEY_TYPE + " in ( \"" + MergeHandel.MERGE_VALUE_TYPE_UPDATE + "\" ,\"" + MergeHandel.MERGE_VALUE_TYPE_INSERT + "\" ) THEN " + //
                    "   UPDATE SET %s ";

    private String assembleMergeSql(SQLBuilder builder, Object finalMergeKeyId, Object mergeKeyIdLast, String mainTableId, String mergeTableId) {
        return String.format(
                MergeHandel.MERGE_SQL,
                mainTableId,//projectAndSetId + this.mainTable.getId(),
                builder.keys(),
                mergeTableId,//projectAndSetId + super.config().tempCursorSchema(),
                finalMergeKeyId,
                mergeKeyIdLast,
                builder.whereSql(),
                builder.insertKeySql(),
                builder.insertValueSql(),
                builder.updateSql());
    }

    private final Object mergeLock = new Object();
    private long mergeDelaySeconds = 300 ;
    private AtomicBoolean running = new AtomicBoolean(false);

    private ScheduledFuture<?> future;

    private TapTable mainTable;
    private StateMapOperator stateMap;

    public MergeHandel mainTable(TapTable mainTable) {
        this.mainTable = mainTable;
        return this;
    }

    public MergeHandel stateMap(StateMapOperator stateMap) {
        this.stateMap = stateMap;
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
        if (Objects.nonNull(this.future)) {
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
//        TapTable eventTable = new TapTable(table.getId(),table.getName());
//        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
//        if (Objects.nonNull(nameFieldMap)&&!nameFieldMap.isEmpty()){
//            for (Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
//                TapField value = entry.getValue();
//                TapField field = new TapField();
//                field.setName(value.getName());
//                field.setComment(value.getComment());
//                field.setPrimaryKey(value.getPrimaryKey());
//                field.setNullable(value.getNullable());
//                eventTable.add(field);
//            }
//        }
//        this.createSchema(eventTable, tableId);
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
        if (Checker.isEmptyCollection(nameFieldMap)) {
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
            //String defaultValue = tapField.getDefaultValue() == null ? "" : tapField.getDefaultValue().toString();
            //if (Checker.isNotEmpty(defaultValue)) {
            //    if (defaultValue.contains("'")) {
            //        defaultValue = defaultValue.replaceAll("'", "\\'");
            //    }
            //    if (tapField.getTapType() instanceof TapNumber) {
            //        defaultValue = defaultValue.trim();
            //    }
            //    structSql.append(" DEFAULT '").append(defaultValue).append("' ");
            //}

            // comment
            String comment = tapField.getComment();
            structSql.append(" OPTIONS (");
            if (Checker.isNotEmpty(comment)) {
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
        if (Checker.isNotEmpty(collateSpecification)) {
            sql.append(" DEFAULT COLLATE ").append(collateSpecification).append(" ");
        }
        sql.append(" OPTIONS ( ");
        if (Checker.isNotEmpty(comment)) {
            comment = comment.replaceAll("'", "\\'");
            sql.append(" description = '").append(comment).append("' ");
        }
        sql.append(" );");
        BigQueryResult bigQueryResult = super.sqlMarker.executeOnce(sql.toString());
        if (Checker.isEmpty(bigQueryResult)) {
            throw new CoreException("Create table error.");
        }
    }

    public List<TapRecordEvent> temporaryEvents(List<TapRecordEvent> eventList, TapTable table) {
        List<TapRecordEvent> insertRecordEvent = new ArrayList<>();
        eventList.stream().filter(Objects::nonNull).forEach(event -> Optional.ofNullable(this.temporaryEvent(event, table)).ifPresent(insertRecordEvent::add));
        return insertRecordEvent;
    }

    public TapInsertRecordEvent temporaryEvent(TapRecordEvent event, TapTable table) {
        TapInsertRecordEvent insert;
        Map<String, Object> after = new HashMap<>();
        String tableId = event.getTableId();
        if (event instanceof TapInsertRecordEvent) {
            TapInsertRecordEvent recordEvent = (TapInsertRecordEvent) event;
            after.put(MergeHandel.MERGE_KEY_TYPE, MergeHandel.MERGE_VALUE_TYPE_INSERT);
            after.put(MergeHandel.MERGE_KEY_DATA_AFTER, recordEvent.getAfter());
            after.put(MergeHandel.MERGE_KEY_DATA_BEFORE, recordEvent.getAfter());
        } else if (event instanceof TapUpdateRecordEvent) {
            TapUpdateRecordEvent recordEvent = (TapUpdateRecordEvent) event;
            after.put(MergeHandel.MERGE_KEY_TYPE, MergeHandel.MERGE_VALUE_TYPE_UPDATE);
            Map<String, Object> beforeMap = recordEvent.getBefore();
            if (Objects.isNull(beforeMap) || beforeMap.isEmpty()) {
                //TapLogger.warn(TAG,String.format("TapUpdateRecordEvent of table [%s] ,before data is empty.",tableId));
                //在after中获取主键的值作为before
                beforeMap = event.getFilter(table.primaryKeys(true));
            }
            //@TODO 主键值不存在时，作为插入
            if (Objects.isNull(beforeMap) || beforeMap.isEmpty()) {
                after.put(MergeHandel.MERGE_KEY_TYPE, MergeHandel.MERGE_VALUE_TYPE_INSERT);
                after.put(MergeHandel.MERGE_KEY_DATA_BEFORE, recordEvent.getAfter());
            } else {
                after.put(MergeHandel.MERGE_KEY_DATA_BEFORE, beforeMap);
                if (Objects.isNull(recordEvent.getAfter()) || recordEvent.getAfter().isEmpty()) {
                    TapLogger.warn(TAG, String.format("TapUpdateRecordEvent of table [%s] ,after data is empty.", tableId));
                }
                after.put(MergeHandel.MERGE_KEY_DATA_AFTER, recordEvent.getAfter());
            }
        } else if (event instanceof TapDeleteRecordEvent) {
            TapDeleteRecordEvent recordEvent = (TapDeleteRecordEvent) event;
            after.put(MergeHandel.MERGE_KEY_TYPE, MergeHandel.MERGE_VALUE_TYPE_DELETE);
            after.put(MergeHandel.MERGE_KEY_DATA_BEFORE, recordEvent.getBefore());
        } else {
            return null;
        }
        long nanoTime = System.nanoTime();
        after.put(MergeHandel.MERGE_KEY_ID, nanoTime);
        after.put(MergeHandel.MERGE_KEY_EVENT_TIME, event.getReferenceTime());
        after.put(MergeHandel.MERGE_KEY_TABLE_ID, event.getTableId());
        insert = new TapInsertRecordEvent();
        insert.after(after);
        insert.referenceTime(event.getReferenceTime());
        insert.setTableId(tableId);
        return insert;
    }

    /**
     * 清空临时表
     */
    public void cleanTemporaryTable(String tableName) {
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
        String cursorSchema = this.stateMap.getString(tableName, ContextConfig.TEMP_CURSOR_SCHEMA_NAME);
        if (Objects.isNull(cursorSchema)) {
            throw new CoreException("Tap table cannot be empty.");
        }
        Object newestRecordId = this.stateMap.getOfTable(tableName, MergeHandel.MERGE_KEY_ID);
        if (Objects.isNull(newestRecordId)) {
            newestRecordId = 0;
        }
        try {
            super.sqlMarker.executeOnce(String.format(
                    MergeHandel.CLEAN_TABLE_SQL,
                    projectId,
                    tableSet,
                    cursorSchema,
                    newestRecordId));
        } catch (Exception e) {
            TapLogger.warn(TAG, "Failed to empty temporary table, table name is " + super.config().tempCursorSchema() + ", " + e.getMessage());
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

    public boolean dropTemporaryTable(List<String> temporaryTableId) {
        if (Objects.isNull(temporaryTableId) || temporaryTableId.isEmpty()) {
            throw new CoreException("Drop event error,table name cannot be empty.");
        }
        try {
            StringJoiner joiner = new StringJoiner(";");
            temporaryTableId.stream().filter(Objects::nonNull).forEach(tab -> {
                joiner.add(String.format(
                        MergeHandel.DROP_TABLE_SQL
                        , super.config().projectId()
                        , super.config().tableSet()
                        , tab)
                );
            });
            BigQueryResult bigQueryResult = super.sqlMarker.executeOnce(joiner.toString());
            return Checker.isNotEmpty(bigQueryResult) && Checker.isNotEmpty(bigQueryResult.getTotalRows()) && bigQueryResult.getTotalRows() > 0;
        } catch (Exception e) {
            throw new CoreException(String.format("Drop temporary table error,table name is: %s, error is: %s.", temporaryTableId.toString(), e.getMessage()));
        }
    }

    public boolean dropTemporaryTableByMainTable(String mainTableId) {
        String cursorSchema = this.stateMap.getString(mainTableId, ContextConfig.TEMP_CURSOR_SCHEMA_NAME);
        if (Objects.isNull(cursorSchema) || "".equals(cursorSchema.trim())) {
            TapLogger.info(TAG, String.format(" A temporary table deletion operation was canceled because a temporary table matching %s table was found ", mainTableId));
        }
        return this.dropTemporaryTable(cursorSchema);
    }

    /**
     * 合并零时表到主表 - 混合模式, 刚启动未merge前设置时间间隔1小时，merge过了之后，按用户配置时间作为时间间隔。
     */
    private boolean hasMerged = false;
    public void mergeTemporaryTableToMainTable(TapTable mainTable) {
        Object hasMergedTime = this.stateMap.get(MergeHandel.HAS_MERGED);
        this.hasMerged = Objects.nonNull(hasMergedTime) && hasMergedTime instanceof Boolean;
        long time = this.hasMerged ? this.mergeDelaySeconds : MergeHandel.DEFAULT_MERGE_DELAY_SECOND;
        synchronized (this){
            if (!this.hasMerged && Objects.isNull(this.future)) {
                long now = System.nanoTime();
                Object batchToStreamTime = this.stateMap.get(MergeHandel.BATCH_TO_STREAM_TIME);
                long defaultTimeSecond = Objects.isNull(batchToStreamTime)? 0 : (now - (Long) batchToStreamTime) / 1000000000L;
                time = defaultTimeSecond > time - this.mergeDelaySeconds ? this.mergeDelaySeconds : time - defaultTimeSecond;
                this.stateMap.save(MergeHandel.HAS_MERGED,now);
            }
        }
        this.mergeTemporaryTableToMainTable(mainTable,time,false);
    }
    private void mergeTemporaryTableToMainTable(TapTable mainTable,long mergeDelaySeconds,boolean needRestart) {
        if (Objects.isNull(mainTable)) {
            throw new CoreException("TableTable must not be null or not be empty.");
        }
        this.mainTable = mainTable;
        this.mergeTable.put(mainTable.getId(), mainTable);
        synchronized (this) {
            if (Objects.isNull(this.future)) {
                this.future = this.scheduledExecutorService.scheduleWithFixedDelay(this.mergeRunnable(), 0, mergeDelaySeconds, TimeUnit.SECONDS);
            }else if (needRestart){
                if (!this.future.isCancelled()){
                    this.future.cancel(true);
                    this.future = null;
                }
                this.future = this.scheduledExecutorService.scheduleWithFixedDelay(
                        this.mergeRunnable(),
                        0,
                        mergeDelaySeconds,
                        TimeUnit.SECONDS
                );
            }
        }
    }
    private Runnable mergeRunnable(){
        return () -> {
            synchronized (this.mergeLock) {
                try {
                    this.mergeTableOnce();
                } catch (Throwable throwable) {
                    TapLogger.warn(TAG, "An exception occurred during data merge, temporary table: {}, target table: {}, {}", this.stateMap.getOfTable(mainTable.getId(), ContextConfig.TEMP_CURSOR_SCHEMA_NAME), mainTable.getId(), throwable.getMessage());
                }
            }
        };
    }

    public void mergeTableOnce() {
        for (Map.Entry<String, TapTable> tableEntry : this.mergeTable.entrySet()) {
            this.mergeTableOnce(tableEntry.getKey());
        }
    }

    /**
     * 全量未完成，返回null,不需要merge
     * return next merge time
     */
    private Long needMergeTime(String tableId) {
        Long streamToBatchTime = this.stateMap.getLong(tableId, MergeHandel.BATCH_TO_STREAM_TIME);
        if (Objects.isNull(streamToBatchTime)) return null;
        Long mergeId = this.stateMap.getLong(tableId, MergeHandel.MERGE_KEY_ID);
        // 启动merge线程
        long delay = MergeHandel.FIRST_MERGE_DELAY_SECOND * 1000000000L;
        long nowTime = System.nanoTime();
        //mergeId == null,判断此时为首次merge,延时33min
        long time = 0L;
        if (Objects.isNull(mergeId)) {
            time = streamToBatchTime + delay;
        } else {
            // -->Time course direction
            // |------------------|---------------------------|---------|----------|--------
            // T1                 T2                          T3       DT1         T4
            // may be :
            // T1: Task start time point .
            // T2: End of batch processing and start time of outflow processing
            // T3: Time point of first data merge
            // T4: Time point of second data merge
            // DT1: Possible recovery time point after task and after merge data interruption , nowTime
            //
            //mergeId != null,非首次merge,是否在上传merge到现在的时间超过了用户配置的时间间隔
            //超过了就merge异一次，未超过这设置时间差进行延时merge.
            Long delaySecond = super.config().mergeDelay();
            time = delaySecond * 1000000000L + mergeId;
        }
        return time > (nowTime + 5000000000L) ? null : time;
    }

    public void mergeTableOnce(String tableName) {
        Long time = this.needMergeTime(tableName);
        if (Objects.isNull(time)) return;
        TapTable tapTable = this.mergeTable.get(tableName);
        if (Objects.isNull(tapTable)) {
            TapLogger.warn(TAG, "A merge operation was rejected.");
        }
        Object mergeKeyId = this.stateMap.getOfTable(tableName, MergeHandel.MERGE_KEY_ID);
        Object mergeKeyIdLast = this.stateMap.getOfTable(tableName, MergeHandel.MERGE_KEY_ID_LAST);
        Object mergeTableId = this.stateMap.getOfTable(tableName, ContextConfig.TEMP_CURSOR_SCHEMA_NAME);
        if (Objects.isNull(mergeKeyIdLast)) {
            mergeKeyIdLast = 0L;
        }
        SQLBuilder builder = this.mergeSql(tapTable);
        String projectAndSetId = super.config().projectId() + "." + super.config().tableSet() + ".";
        Object finalMergeKeyId = Objects.isNull(mergeKeyId) ? 0 : mergeKeyId;
        try {
            if (Long.parseLong(String.valueOf(finalMergeKeyId)) < Long.parseLong(String.valueOf(mergeKeyIdLast))) {
                mergeKeyIdLast = 0L;
            }
        } catch (Exception e) {
            mergeKeyIdLast = 0L;
        }
        try {
            BigQueryResult bigQueryResult = super.sqlMarker.executeOnce(this.assembleMergeSql(builder, finalMergeKeyId, mergeKeyIdLast, projectAndSetId + tableName, projectAndSetId + String.valueOf(mergeTableId)));
            List<Map<String, Object>> result = bigQueryResult.result();
            long totalRows = bigQueryResult.getTotalRows();
            TapLogger.info(TAG, String.format("Data consolidation has been performed. Merge to table: %s, temporary table: %s, merge result: %s rows, %s.", tapTable.getId(), mergeTableId, totalRows, toJson(result)));
            this.stateMap.saveForTable(tableName, MergeHandel.MERGE_KEY_ID_LAST, finalMergeKeyId);
            this.cleanTemporaryTable(tableName);
        }catch (Exception e){
            throw new RuntimeException(e.getMessage());
        }finally {
            if (!this.hasMerged) {
                this.hasMerged = true;
                this.stateMap.save(MergeHandel.HAS_MERGED, true);
                this.mergeTemporaryTableToMainTable(tapTable, this.mergeDelaySeconds, true);
            }
        }
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
        StringJoiner keySql = new StringJoiner(MergeHandel.DELIMITER);
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
                keySql.add("delta.merge_data_before." + key);
            });
        }
        return new SQLBuilder(
                whereSql.toString(),
                insertKeySql.toString(),
                insertValueSql.toString(),
                updateSql.toString(),
                keySql.toString());
    }

    static class SQLBuilder {
        String whereSql;
        String insertKeySql;
        String insertValueSql;
        String updateSql;
        String keys;

        public String keys() {
            return this.keys;
        }

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

        public SQLBuilder(String whereSql, String insertKeySql, String insertValueSql, String updateSql, String keys) {
            this.whereSql = whereSql;
            this.insertKeySql = insertKeySql;
            this.updateSql = updateSql;
            this.insertValueSql = insertValueSql;
            this.keys = keys;
        }
    }
}
