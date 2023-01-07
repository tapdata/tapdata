package io.tapdata.bigquery.service.bigQuery;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.tapdata.bigquery.entity.ContextConfig;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.Objects;
import java.util.UUID;


public abstract class BigQueryStart {
    public static final String TAG = BigQueryStart.class.getSimpleName();

    protected ContextConfig config;
    protected TapConnectionContext connectorContext;
    protected SqlMarker sqlMarker;

    public BigQueryStart(TapConnectionContext connectorContext) {
        this.connectorContext = connectorContext;
        this.config = config();
        this.sqlMarker = SqlMarker.create(config.serviceAccount());
    }

    public static ContextConfig config(TapConnectionContext connectorContext) {
        ContextConfig contextConfig = ContextConfig.create();
        if (null == connectorContext) return contextConfig;
        DataMap connectionConfig = connectorContext.getConnectionConfig();
        String serviceAccount = connectionConfig.getString("serviceAccount");
        if (null == serviceAccount || "".equals(serviceAccount)) {
            throw new CoreException("Credentials is must not be null or not be empty.");
        }
        try {
            JSONObject parse = JSONUtil.parseObj(serviceAccount);
            Object projectId = parse.get("project_id");
            if (null == projectId) {
                throw new CoreException("Credentials is must not be null or not be empty.");
            }
            contextConfig.projectId(String.valueOf(projectId));
        } catch (Exception e) {
            throw new CoreException("Credentials is must not be null or not be empty, can not get project id.");
        }
        return contextConfig.serviceAccount(serviceAccount);
    }

    public ContextConfig config() {
        ContextConfig contextConfig = ContextConfig.create();
        if (Objects.isNull(this.connectorContext)) return contextConfig;
        DataMap connectionConfig = this.connectorContext.getConnectionConfig();

        String serviceAccount = connectionConfig.getString("serviceAccount");
        if (null == serviceAccount || "".equals(serviceAccount)) {
            throw new CoreException("Credentials cannot be empty.");
        }
        try {
            JSONObject parse = JSONUtil.parseObj(serviceAccount);
            Object projectId = parse.get("project_id");
            if (null == projectId) {
                throw new CoreException("Credentials cannot be empty.");
            }
            contextConfig.projectId(String.valueOf(projectId));
        } catch (Exception e) {
            throw new CoreException("Credentials cannot be empty, can not get project id.");
        }


        String tableSet = connectionConfig.getString("tableSet");
        if (null == tableSet || "".equals(tableSet)) {
            throw new CoreException("Credentials cannot be empty.");
        }
        DataMap nodeConfig = this.connectorContext.getNodeConfig();
        if (connectorContext instanceof TapConnectorContext && Objects.nonNull(nodeConfig)) {
            String writeMode = nodeConfig.getString("writeMode");
            if (null == writeMode || "".equals(writeMode)) {
                throw new CoreException("WriteMode cannot be empty.");
            }
            contextConfig.writeMode(writeMode);
            if ("MIXED_UPDATES".equals(writeMode)) {
                String cursorSchema = nodeConfig.getString("cursorSchema");
                if (null == cursorSchema || "".equals(cursorSchema)) {
                    throw new CoreException("WriteMode is MIXED_UPDATES and CursorSchema cannot be empty.");
                }
                contextConfig.cursorSchema(cursorSchema);
                if (connectorContext instanceof TapConnectorContext) {
                    TapConnectorContext context = (TapConnectorContext) connectorContext;
                    KVMap<Object> stateMap = context.getStateMap();
                    Object tempCursorSchema = stateMap.get(ContextConfig.TEMP_CURSOR_SCHEMA_NAME);
                    if (Objects.isNull(tempCursorSchema)) {
                        tempCursorSchema = cursorSchema + "_" + UUID.randomUUID().toString().replaceAll("-", "_");
                        stateMap.put(ContextConfig.TEMP_CURSOR_SCHEMA_NAME, tempCursorSchema);
                        TapLogger.info(TAG, "Cache Schema has created ,named is " + tempCursorSchema);
                    }
                    contextConfig.tempCursorSchema(String.valueOf(tempCursorSchema));
                }
                long time;
                try {
                    String mergeDelay = nodeConfig.getString("mergeDelay");
                    time = Long.parseLong(mergeDelay);
                } catch (Exception e) {
                    time = 3600L;
                }
                contextConfig.mergeDelay(time);
            }
        }
        return contextConfig.serviceAccount(serviceAccount).tableSet(tableSet);
    }
}
