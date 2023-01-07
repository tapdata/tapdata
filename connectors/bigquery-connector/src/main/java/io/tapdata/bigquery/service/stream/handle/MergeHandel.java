package io.tapdata.bigquery.service.stream.handle;

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
    public static final String MERGE_KEY_TYPE = "merge_type";
    public static final String MERGE_KEY_DATA_BEFORE = "merge_data_before";
    public static final String MERGE_KEY_DATA_AFTER = "merge_data_after";
    public static final String MERGE_KEY_EVENT_TIME = "record_event_time";
    public static final String MERGE_KEY_TABLE_ID = "record_table_id";

    public static final String MERGE_VALUE_TYPE_INSERT = "I";
    public static final String MERGE_VALUE_TYPE_UPDATE = "U";
    public static final String MERGE_VALUE_TYPE_DELETE = "D";

    private String temporaryTableId;
    private final Object mergeLock = new Object();

    public static final String DELIMITER = "," ;
    private long mergeDelaySeconds = 2*60*60;

    AtomicBoolean running = new AtomicBoolean(false);
    private ScheduledFuture<?> future;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public String temporaryTableId(){
        return this.temporaryTableId;
    }
    public MergeHandel temporaryTableId(String temporaryTableId){
        this.temporaryTableId = temporaryTableId;
        return this;
    }
    public MergeHandel running(AtomicBoolean run){
        this.running = run;
        return this;
    }
    public MergeHandel mergeDelaySeconds(long mergeDelaySeconds){
        this.mergeDelaySeconds = mergeDelaySeconds;
        return this;
    }
    public Object mergeLock(){
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

    public static MergeHandel merge(TapConnectionContext connectorContext){
        return new MergeHandel(connectorContext);
    }

    /**
     * 创建临时表
     * */
    public TapTable createTemporaryTable(TapTable table,String tableId){
        TableCreate tableCreate = TableCreate.create(this.connectorContext);
        TapTable temporaryTable = table(tableId)
                .add(field(MERGE_KEY_ID,JAVA_Long).isPrimaryKey(true).primaryKeyPos(1).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field(MERGE_KEY_TYPE,JAVA_String).tapType(tapString().bytes(10L)))
                .add(field(MERGE_KEY_DATA_BEFORE, JAVA_Map).tapType(tapMap()))
                .add(field(MERGE_KEY_DATA_AFTER, JAVA_Map).tapType(tapMap()))
                .add(field(MERGE_KEY_EVENT_TIME,JAVA_Long).isPrimaryKey(true).primaryKeyPos(1).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field(MERGE_KEY_TABLE_ID,JAVA_String).tapType(tapString().bytes(1024L)));
        TapCreateTableEvent event = new TapCreateTableEvent();
        event.setTableId(tableId);
        event.setTable(temporaryTable);
        event.setReferenceTime(System.currentTimeMillis());
        if (tableCreate.isExist(event)){
            TapLogger.info(TAG,"Temporary table ["+temporaryTableId+"] already exists");
            return table;
        }
        createSchema(table,tableId);
        return table;
    }

    /**
     *   x INT64 OPTIONS(description="An optional INTEGER field"),
     *   y STRUCT<
     *     a ARRAY<STRING> OPTIONS(description="A repeated STRING field"),
     *     b BOOL
     *   >
     * */
    private void createSchema(TapTable table,String tableId){
        if (Checker.isEmpty(this.config)){
            throw new CoreException("Connection config is null or empty.");
        }
        String tableSet = this.config.tableSet();
        if (Checker.isEmpty(tableSet)){
            throw new CoreException("Table set is null or empty.");
        }
        if (Checker.isEmpty(table)){
            throw new CoreException("Tap table is null or empty.");
        }

        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        if (io.tapdata.bigquery.util.tool.Checker.isEmptyCollection(nameFieldMap)){
            throw new CoreException("Tap table schema null or empty.");
        }
        String tableSetName = "`"+tableSet+"`.`"+tableId+"`";

        StringBuilder sql = new StringBuilder();//" DROP TABLE IF EXISTS ")
        sql.append("CREATE TABLE")
                .append(tableSetName)
                .append(" (")
                .append(MERGE_KEY_ID).append(" INT64 OPTIONS(description=\"An optional INTEGER field\"),")
                .append(MERGE_KEY_EVENT_TIME).append(" INT64 OPTIONS(description=\"An optional INTEGER field\"),")
                .append(MERGE_KEY_TABLE_ID).append(" STRING OPTIONS(description=\"An optional INTEGER field\"),")
                .append(MERGE_KEY_TYPE).append(" STRING OPTIONS(description=\"I/U/D, is TapEventType\"),");

        StringBuilder structSql = new StringBuilder();
        nameFieldMap.forEach((key,tapField)->{
            String dataType = tapField.getDataType();
            structSql.append(" `")
                    .append(key)
                    .append("` ")
                    .append(dataType.toUpperCase());
            //DEFAULT
            String defaultValue = tapField.getDefaultValue() == null ? "" : tapField.getDefaultValue().toString();
            if (io.tapdata.bigquery.util.tool.Checker.isNotEmpty(defaultValue)) {
                if(defaultValue.contains("'")){
                    defaultValue = defaultValue.replaceAll( "'", "\\'");
                }
                if(tapField.getTapType() instanceof TapNumber){
                    defaultValue = defaultValue.trim();
                }
                structSql.append(" DEFAULT '").append(defaultValue).append("' ");
            }

            // comment
            String comment = tapField.getComment();
            structSql.append(" OPTIONS (");
            if (io.tapdata.bigquery.util.tool.Checker.isNotEmpty(comment)) {
                comment = comment.replaceAll("'", "\\'");
                structSql.append(" description = '").append( comment).append("' ");
            }
            //if has next option please split by comment [,]
            structSql.append(" ),");
        });
        if (structSql.lastIndexOf(",")==structSql.length()-1) {
            structSql.deleteCharAt(structSql.lastIndexOf(","));
        }

        sql.append(MERGE_KEY_DATA_BEFORE).append(" STRUCT<").append(structSql).append(">,")
           .append(MERGE_KEY_DATA_AFTER).append(" STRUCT<").append(structSql).append(">");

        String comment = table.getComment();
        sql.append(" ) ");
        String collateSpecification = "";//默认排序规则
        if (io.tapdata.bigquery.util.tool.Checker.isNotEmpty(collateSpecification)){
            sql.append(" DEFAULT COLLATE ").append(collateSpecification).append(" ");
        }
        sql.append(" OPTIONS ( ");
        if (io.tapdata.bigquery.util.tool.Checker.isNotEmpty(comment)) {
            comment = comment.replaceAll("'", "\\'");
            sql.append(" description = '").append(comment).append("' ");
        }
        sql.append(" );");
        BigQueryResult bigQueryResult = sqlMarker.executeOnce(sql.toString());
        if (Checker.isEmpty(bigQueryResult)){
            throw new CoreException("Create table error.");
        }
    }

    public List<TapRecordEvent> temporaryEvent(List<TapRecordEvent> eventList){
        List<TapRecordEvent> insertRecordEvent = new ArrayList<>();
        for (TapRecordEvent event : eventList) {
            TapInsertRecordEvent insert ;
            Map<String,Object> after = new HashMap<>();
            if (event instanceof TapInsertRecordEvent){
                TapInsertRecordEvent recordEvent = (TapInsertRecordEvent) event;
                after.put(MERGE_KEY_TYPE, MERGE_VALUE_TYPE_INSERT);
                after.put(MERGE_KEY_DATA_AFTER,recordEvent.getAfter());
            }else if (event instanceof TapUpdateRecordEvent){
                TapUpdateRecordEvent recordEvent = (TapUpdateRecordEvent) event;
                after.put(MERGE_KEY_TYPE, MERGE_VALUE_TYPE_UPDATE);
                after.put(MERGE_KEY_DATA_BEFORE,recordEvent.getBefore());
                after.put(MERGE_KEY_DATA_AFTER,recordEvent.getAfter());

            }else if (event instanceof TapDeleteRecordEvent){
                TapDeleteRecordEvent recordEvent = (TapDeleteRecordEvent) event;
                after.put(MERGE_KEY_TYPE, MERGE_VALUE_TYPE_DELETE);
                after.put(MERGE_KEY_DATA_BEFORE,recordEvent.getBefore());
            }else {
                continue;
            }
            long nanoTime = System.nanoTime();
            after.put(MERGE_KEY_ID, nanoTime);
            after.put(MERGE_KEY_EVENT_TIME,event.getReferenceTime());
            after.put(MERGE_KEY_TABLE_ID,event.getTableId());
            insert = new TapInsertRecordEvent();
            insert.after(after);
            insert.referenceTime(event.getReferenceTime());
            insert.setTableId(temporaryTableId);
            insertRecordEvent.add(insert);
        }
        return insertRecordEvent;
    }

    /**
     * 清空临时表
     * */
    private static final String CLEAN_TABLE_SQL = "DELETE FROM `%s`.`%s`.`%s` WHERE id <= %s;";
    public void cleanTemporaryTable(){
        if (Checker.isEmpty(this.config)){
            throw new CoreException("Connection config is null or empty.");
        }
        String projectId = this.config.projectId();
        if (Checker.isEmpty(projectId)){
            throw new CoreException("Project ID is null or empty.");
        }
        String tableSet = this.config.tableSet();
        if (Checker.isEmpty(tableSet)){
            throw new CoreException("Table set is null or empty.");
        }
        if (Checker.isEmpty(temporaryTableId)){
            throw new CoreException("Tap table is null or empty.");
        }
        KVMap<Object> stateMap = ((TapConnectorContext) this.connectorContext).getStateMap();
        Object newestRecordId = stateMap.get(MERGE_KEY_ID);
        if (Objects.isNull(newestRecordId)){
            newestRecordId = 0;
        }
        try {
            this.sqlMarker.executeOnce(String.format(CLEAN_TABLE_SQL,projectId,tableSet,temporaryTableId,newestRecordId));
        }catch (Exception e){
            throw new CoreException("Failed to empty temporary table, table name is " + temporaryTableId + ", " + e.getMessage());
        }
    }

    private final static String DROP_TABLE_SQL = "DROP TABLE IF EXISTS `%s`.`%s`.`%s`;";
    public boolean dropTemporaryTable(String temporaryTableId){
        if (Checker.isEmpty(temporaryTableId)){
            throw new CoreException("Drop event error,table name must be null or be empty.");
        }
        try {
            BigQueryResult bigQueryResult = this.sqlMarker.executeOnce(
                    String.format(DROP_TABLE_SQL
                            , this.config.projectId()
                            , this.config.tableSet()
                            , temporaryTableId));
            return Checker.isNotEmpty(bigQueryResult) && Checker.isNotEmpty(bigQueryResult.getTotalRows()) && bigQueryResult.getTotalRows()>0;
        }catch (Exception e){
            throw new CoreException(String.format("Drop temporary table error,table name is: %s, error is: %s.",temporaryTableId,e.getMessage()));
        }
    }

    /**
     * 合并零时表到主表 - 混合模式
     * */
    TapTable mainTable;
    public void mergeTemporaryTableToMainTable(TapTable mainTable){
        if (Objects.isNull(mainTable)){
            throw new CoreException("TableTable must not be null or not be empty.");
        }
        this.mainTable = mainTable;
        if (Objects.isNull(future)) {
            future = scheduledExecutorService.scheduleWithFixedDelay(() -> {
                synchronized (mergeLock){
                    try {
                        this.mergeTableOnce();
                    } catch (Throwable throwable) {
                        TapLogger.error(TAG, "Try upload failed in scheduler, {}", throwable.getMessage());
                    }
                }
            }, 10, mergeDelaySeconds, TimeUnit.SECONDS);
        }
    }
    private void mergeTableOnce(){
        KVMap<Object> stateMap = ((TapConnectorContext) this.connectorContext).getStateMap();
        Object mergeKeyId = stateMap.get(MergeHandel.MERGE_KEY_ID);
        SQLBuilder builder = mergeSql(mainTable);
        String projectAndSetId = config().projectId()+"."+config().tableSet()+".";
        Object finalMergeKeyId = Objects.isNull(mergeKeyId) ? 0 : mergeKeyId;
        String sql = String.format(
                MERGE_SQL,
                projectAndSetId + mainTable.getId(),
                projectAndSetId + temporaryTableId,
                finalMergeKeyId,
                builder.whereSql(),
                builder.insertKeySql(),
                builder.insertValueSql(),
                finalMergeKeyId,
                builder.updateSql());
        this.sqlMarker.executeOnce(sql);
        this.cleanTemporaryTable();
    }

    public static final String MERGE_SQL =
            " MERGE `%s` merge_tab USING( " +
            "SELECT * FROM `%s` targeted WHERE targeted." +MERGE_KEY_ID +"<=%s"+
            //"    SELECT * EXCEPT(row_num) FROM ( " +
            //"        SELECT *, ROW_NUMBER() OVER(PARTITION BY delta."+MERGE_KEY_TYPE+" ORDER BY delta."+MERGE_KEY_ID+" DESC) AS row_num " +
            //"        FROM `%s` delta " +
            //"    ) " +
            //"    WHERE row_num = 1 " +
            " ) tab ON %s " + //merge_tab.id = tab.id
            " WHEN NOT MATCHED AND tab."+MERGE_KEY_TYPE+" in(\""+MERGE_VALUE_TYPE_INSERT+"\",\""+MERGE_VALUE_TYPE_UPDATE+"\") THEN " +
            "   INSERT (%s) VALUES (%s) " + //id ,username, change_id       tab.id,tab.username,tab.change_id
            " WHEN MATCHED AND tab."+MERGE_KEY_TYPE+" = \""+MERGE_VALUE_TYPE_DELETE+"\" THEN " +
            "   DELETE " +
            " WHEN MATCHED AND tab."+MERGE_KEY_TYPE+" = \""+MERGE_VALUE_TYPE_UPDATE+"\" AND (tab."+MERGE_KEY_ID+" < %s) THEN " + //
            "   UPDATE SET %s "; //username = tab.username, change_id = tab.change_id

    public SQLBuilder mergeSql(TapTable table){
        if (Objects.isNull(table)) {
            throw new CoreException("TapTable must not be null or not be empty.");
        }
        Map<String, TapField> fieldMap = table.getNameFieldMap();
        if (Objects.isNull(fieldMap) || fieldMap.isEmpty()) {
            throw new CoreException("TapTable's field map must not be null or not be empty, table name is "+ table.getId() + ".");
        }
        Collection<String> primaryKeys = table.primaryKeys(true);
        boolean hasPrimaryKeys = Objects.nonNull(primaryKeys) && !primaryKeys.isEmpty();
        StringJoiner whereSql = new StringJoiner(" AND ");
        StringJoiner insertKeySql = new StringJoiner(DELIMITER);
        StringJoiner insertValueSql = new StringJoiner(DELIMITER);
        StringJoiner updateSql = new StringJoiner(DELIMITER);
        fieldMap.forEach((key,field)->{
            insertKeySql.add(key);
            insertValueSql.add("tab."+MERGE_KEY_DATA_AFTER+"."+key);
            updateSql.add(key + " = tab."+ MERGE_KEY_DATA_AFTER + "." + key);
            if (!hasPrimaryKeys){
                whereSql.add("tab." + MERGE_KEY_DATA_BEFORE + "." +key+ " = merge_tab."+key);
            }
        });
        if (hasPrimaryKeys){
            primaryKeys.stream().filter(Objects::nonNull).forEach(key->{
                whereSql.add("tab." + MERGE_KEY_DATA_BEFORE + "." +key+ " = merge_tab."+key);
            });
        }
        return new SQLBuilder(whereSql.toString(), insertKeySql.toString(), insertValueSql.toString(), updateSql.toString());
    }

    static class SQLBuilder{
        String whereSql;
        String insertKeySql;
        String insertValueSql;
        String updateSql;
        public String whereSql(){
            return this.whereSql;
        }
        public String insertKeySql(){
            return this.insertKeySql;
        }
        public String insertValueSql(){
            return this.insertValueSql;
        }
        public String updateSql(){
            return this.updateSql;
        }
        public SQLBuilder(String whereSql,String insertKeySql,String insertValueSql,String updateSql){
            this.whereSql = whereSql;
            this.insertKeySql = insertKeySql;
            this.updateSql = updateSql;
            this.insertValueSql = insertValueSql;
        }
    }
}
