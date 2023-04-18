package io.tapdata.connector.tencent.db.core;

import io.tapdata.connector.mysql.MysqlReader;
import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import io.tapdata.connector.mysql.entity.MysqlStreamOffset;
import io.tapdata.connector.tencent.db.mysql.MysqlJdbcContext;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
}
