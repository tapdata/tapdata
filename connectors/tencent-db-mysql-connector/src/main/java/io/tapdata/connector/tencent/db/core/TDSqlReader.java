package io.tapdata.connector.tencent.db.core;

import io.tapdata.connector.mysql.MysqlReader;
import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import io.tapdata.connector.mysql.entity.MysqlStreamEvent;
import io.tapdata.connector.mysql.entity.MysqlStreamOffset;
import io.tapdata.connector.tencent.db.mysql.MysqlJdbcContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.*;

import static io.tapdata.base.ConnectorBase.entry;
import static io.tapdata.base.ConnectorBase.map;

/**
 * @author GavinXiao
 * @description TDSqlReader create by Gavin
 * @create 2023/4/18 14:35
 **/
public class TDSqlReader extends MysqlReader {
    private final String partitionSetId;
    public static final String SERVER_NAME_KEY = "PARTITION_KEY_NAME_CONFIG";

    public TDSqlReader(MysqlJdbcContext mysqlJdbcContext) {
        super(mysqlJdbcContext);
        this.partitionSetId = mysqlJdbcContext.getPartitionSetId();
    }

    @Override
    protected synchronized void initDebeziumServerName(TapConnectorContext tapConnectorContext) {
        super.initDebeziumServerName(tapConnectorContext);
        if (null == this.partitionSetId) {
            super.initDebeziumServerName(tapConnectorContext);
            return;
        }
        serverName = UUID.randomUUID().toString().toLowerCase();
        KVMap<Object> stateMap = tapConnectorContext.getStateMap();
        Object serverNameFromStateMap = stateMap.get(SERVER_NAME_KEY);
        if (serverNameFromStateMap instanceof Map) {
            Map<String, Object> serverNameFromServer = (Map<String, Object>) serverNameFromStateMap;
            Object configMap = serverNameFromServer.get(partitionSetId);
            if (null == configMap) {
                serverNameFromServer.put(partitionSetId, map(
                        entry(MysqlReader.SERVER_NAME_KEY, serverName),
                        entry(MysqlReader.FIRST_TIME_KEY, true)));
            } else {
                Map<String, Object> objectMap = (Map<String, Object>) configMap;
                this.serverName = (String) objectMap.get(MysqlReader.SERVER_NAME_KEY);
                objectMap.put(MysqlReader.FIRST_TIME_KEY, false);
            }
            stateMap.put(SERVER_NAME_KEY, serverNameFromServer);
        } else {
            stateMap.put(SERVER_NAME_KEY, map(
                    entry(partitionSetId, map(
                            entry(MysqlReader.SERVER_NAME_KEY, serverName),
                            entry(MysqlReader.FIRST_TIME_KEY, true))
                    )
            ));
        }
    }

    @Override
    protected MysqlStreamOffset binlogPosition2MysqlStreamOffset(MysqlBinlogPosition offset, JsonParser jsonParser) throws Throwable {
        String serverId = mysqlJdbcContext.getServerId();
        Map<String, Object> partitionMap = new HashMap<>();
        partitionMap.put("server", serverName);
        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put("file", offset.getFilename());
        offsetMap.put("pos", offset.getPosition());
        offsetMap.put("server_id", serverId);
        MysqlStreamOffset mysqlStreamOffset = new MysqlStreamOffset();
        mysqlStreamOffset.setOffsetMap(serverName, new HashMap<String, String>() {{
            put(jsonParser.toJson(partitionMap), jsonParser.toJson(offsetMap));
        }});
        //mysqlStreamOffset.setName(serverName);
        //mysqlStreamOffset.setOffset();
        return mysqlStreamOffset;
    }

    @Override
    protected void sourceRecordConsumer(SourceRecord record) {
        if (null != throwableAtomicReference.get()) {
            throw new RuntimeException(throwableAtomicReference.get());
        }
        if (null == record || null == record.value()) return;
        Schema valueSchema = record.valueSchema();
        List<MysqlStreamEvent> mysqlStreamEvents = new ArrayList<>();
        if (null != valueSchema.field("op")) {
            MysqlStreamEvent mysqlStreamEvent = wrapDML(record);
            Optional.ofNullable(mysqlStreamEvent).ifPresent(mysqlStreamEvents::add);
        } else if (null != valueSchema.field("ddl")) {
            mysqlStreamEvents = wrapDDL(record);
        } else if ("io.debezium.connector.common.Heartbeat".equals(valueSchema.name())) {
            Optional.ofNullable((Struct) record.value())
                    .map(value -> value.getInt64("ts_ms"))
                    .map(TapSimplify::heartbeatEvent)
                    .map(heartbeatEvent -> new MysqlStreamEvent(heartbeatEvent, getMysqlStreamOffset(record)))
                    .ifPresent(mysqlStreamEvents::add);
        }
        if (CollectionUtils.isNotEmpty(mysqlStreamEvents)) {
            List<TapEvent> tapEvents = new ArrayList<>();
            MysqlStreamOffset mysqlStreamOffset = null;
            KVReadOnlyMap<TapTable> tableMap = ((TapConnectorContext) mysqlJdbcContext.getTapConnectionContext()).getTableMap();
            for (MysqlStreamEvent mysqlStreamEvent : mysqlStreamEvents) {
                TapEvent tapEvent = mysqlStreamEvent.getTapEvent();
                if (tapEvent instanceof TapInsertRecordEvent) {
                    Map<String, Object> after = ((TapInsertRecordEvent) tapEvent).getAfter();
                    TapTable tapTable = tableMap.get(((TapInsertRecordEvent) tapEvent).getTableId());
                    LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
                    nameFieldMap.entrySet().stream().filter(ent -> {
                        TapField value = ent.getValue();
                        return null != value.getDataType() && "YEAR".equals(value.getDataType().toUpperCase(Locale.ROOT));
                    }).forEach(entry -> {
                        Object o = after.get(entry.getKey());
                        if (o instanceof Integer){
                            after.put(entry.getKey(), "" + o);
                        }
                    });
                } else if (tapEvent instanceof TapUpdateRecordEvent) {
                    Map<String, Object> after = ((TapUpdateRecordEvent) tapEvent).getAfter();
                    TapTable tapTable = tableMap.get(((TapUpdateRecordEvent) tapEvent).getTableId());
                    LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
                    nameFieldMap.entrySet().stream().filter(ent -> {
                        TapField value = ent.getValue();
                        return null != value.getDataType() && "YEAR".equals(value.getDataType().toUpperCase(Locale.ROOT));
                    }).forEach(entry -> {
                        Object o = after.get(entry.getKey());
                        if (o instanceof Integer){
                            after.put(entry.getKey(), "" + o);
                        }
                    });
                }
                tapEvents.add(tapEvent);
                mysqlStreamOffset = mysqlStreamEvent.getMysqlStreamOffset();
            }
            streamReadConsumer.accept(tapEvents, mysqlStreamOffset);
        }
    }
}
