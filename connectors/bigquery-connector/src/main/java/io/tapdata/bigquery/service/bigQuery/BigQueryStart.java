package io.tapdata.bigquery.service.bigQuery;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.tapdata.bigquery.entity.ContextConfig;
import io.tapdata.bigquery.service.stream.v2.StateMapOperator;
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

    public ContextConfig config() {
        return this.config;
    }

    public BigQueryStart(TapConnectionContext connectorContext) {
        this.connectorContext = connectorContext;
    }

    public BigQueryStart autoStart() {
        this.config = this.contextConfig();
        this.sqlMarker = SqlMarker.create(this.config.serviceAccount());
        return this;
    }

    public BigQueryStart paperStart(BigQueryStart starter) {
        if (Objects.isNull(starter)) {
            throw new CoreException("The preload object cannot be empty according to the supplementary operation class attribute of the load data. ");
        }
        this.config = starter.config();
        this.sqlMarker = starter.sqlMarker;
        return this;
    }

    public static ContextConfig contextConfig(TapConnectionContext connectorContext) {
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

    public ContextConfig contextConfig() {
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

        Object maxStreamAppendCount = connectionConfig.get("maxStreamAppendCount");
        int maxStreamCount;
        if (Objects.isNull(maxStreamAppendCount)){
            maxStreamCount = 50000;
        }else {
            try {
                maxStreamCount = Integer.parseInt(String.valueOf(maxStreamAppendCount));
                if (maxStreamCount <= 0 || maxStreamCount > 50000) {
                    maxStreamCount = 50000;
                }
            } catch (Exception e) {
                maxStreamCount = 50000;
            }
        }
        contextConfig.maxStreamAppendCount(maxStreamCount);

        this.$v2_config(contextConfig);
        //this.$v1_config(contextConfig);
        return contextConfig.serviceAccount(serviceAccount).tableSet(tableSet);
    }

    /**
     * @deprecated
     */
    private void $v1_config(ContextConfig contextConfig) {
        DataMap nodeConfig = this.connectorContext.getNodeConfig();
        if (connectorContext instanceof TapConnectorContext && Objects.nonNull(nodeConfig)) {
            String writeMode = nodeConfig.getString("writeMode");
            if (null == writeMode || "".equals(writeMode)) {
                throw new CoreException("WriteMode cannot be empty.");
            }
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
            contextConfig.writeMode(writeMode);
        }
    }

    private void $v2_config(ContextConfig contextConfig) {
        DataMap nodeConfig = this.connectorContext.getNodeConfig();
        if (connectorContext instanceof TapConnectorContext && Objects.nonNull(nodeConfig)) {
            String cursorSchema = nodeConfig.getString("cursorSchema");
            if (Objects.isNull(cursorSchema) || "".equals(cursorSchema.trim())) {
                cursorSchema = ContextConfig.TEMP_CURSOR_SCHEMA_NAME_DEFAULT;
                TapLogger.info(TAG, "The temporary table is named, and the default name has been selected as the prefix , named is " + cursorSchema);
            }
            contextConfig.cursorSchema(cursorSchema);
            TapConnectorContext context = (TapConnectorContext) connectorContext;
            KVMap<Object> stateMap = context.getStateMap();
            Object tempCursorSchema = stateMap.get(ContextConfig.TEMP_CURSOR_SCHEMA_NAME);
            if (Objects.isNull(tempCursorSchema)) {
                tempCursorSchema = cursorSchema + "_" + UUID.randomUUID().toString().replaceAll("-", "_");
                //stateMap.put(ContextConfig.TEMP_CURSOR_SCHEMA_NAME, tempCursorSchema); //这里不需要put,已改变到 临时表建表时put
                TapLogger.info(TAG, "Cache Schema has created ,named is " + tempCursorSchema);
            }
            contextConfig.tempCursorSchema(String.valueOf(tempCursorSchema));

            long time;
            try {
                String mergeDelay = nodeConfig.getString("mergeDelay");
                time = Long.parseLong(mergeDelay);
            } catch (Exception e) {
                time = ContextConfig.MERGE_DELAY_DEFAULT;
                TapLogger.info(TAG, " The merge delay interval is specified, and the default interval has been selected ,it is: " + time + "s.");
            }
            contextConfig.mergeDelay(time);
        }
    }

    protected String tempCursorSchema(String tableId, StateMapOperator stateMap){
        Object tempCursorSchema = stateMap.getString(tableId,ContextConfig.TEMP_CURSOR_SCHEMA_NAME);
        if (Objects.isNull(tempCursorSchema)) {
            if (connectorContext instanceof TapConnectorContext ){
                DataMap nodeConfig = this.connectorContext.getNodeConfig();
                if(Objects.isNull(nodeConfig)){
                    //TapLogger.error(TAG," Before creating a temporary table, you need to obtain the temporary table prefix name, but NodeConfig is empty .");
                    //@TODO
                    nodeConfig = new DataMap();
                    nodeConfig.put("cursorSchema",ContextConfig.TEMP_CURSOR_SCHEMA_NAME_DEFAULT);
                }
                String cursorSchema = nodeConfig.getString("cursorSchema");
                if (Objects.isNull(cursorSchema) || "".equals(cursorSchema.trim())) {
                    cursorSchema = ContextConfig.TEMP_CURSOR_SCHEMA_NAME_DEFAULT;
                    TapLogger.info(TAG, "The temporary table is named, and the default name has been selected as the prefix , named is " + cursorSchema);
                }
                this.config.cursorSchema(cursorSchema);

                long time;
                try {
                    String mergeDelay = nodeConfig.getString("mergeDelay");
                    time = Long.parseLong(mergeDelay);
                } catch (Exception e) {
                    time = ContextConfig.MERGE_DELAY_DEFAULT;
                    TapLogger.info(TAG, " The merge delay interval is specified, and the default interval has been selected ,it is: " + time + "s.");
                }
                this.config.mergeDelay(time);
            }
            tempCursorSchema = this.config().cursorSchema() + "_" + UUID.randomUUID().toString().replaceAll("-", "_");
            TapLogger.info(TAG, "Cache Schema has created ,named is " + tempCursorSchema);
        }
        return String.valueOf(tempCursorSchema);
    }
    public void setConfig(){}

}
