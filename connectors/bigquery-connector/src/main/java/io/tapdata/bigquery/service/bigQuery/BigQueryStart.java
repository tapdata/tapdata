package io.tapdata.bigquery.service.bigQuery;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.tapdata.bigquery.entity.ContextConfig;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.Objects;


public abstract class BigQueryStart {
    protected ContextConfig config;
    protected TapConnectionContext connectorContext;
    protected SqlMarker sqlMarker;
    public BigQueryStart(TapConnectionContext connectorContext) {
        this.connectorContext = connectorContext;
        this.config = config();
        this.sqlMarker = SqlMarker.create(config.serviceAccount());
    }
    public static ContextConfig config(TapConnectionContext connectorContext){
        ContextConfig contextConfig = ContextConfig.create();
        if (null == connectorContext) return contextConfig;
        DataMap connectionConfig = connectorContext.getConnectionConfig();
        String serviceAccount = connectionConfig.getString("serviceAccount");
        if (null == serviceAccount || "".equals(serviceAccount)){
            throw new CoreException("Credentials is must not be null or not be empty.");
        }
        try {
            JSONObject parse = JSONUtil.parseObj(serviceAccount);
            Object projectId = parse.get("project_id");
            if (null == projectId ){
                throw new CoreException("Credentials is must not be null or not be empty.");
            }
            contextConfig.projectId(String.valueOf(projectId));
        }catch (Exception e){
            throw new CoreException("Credentials is must not be null or not be empty, can not get project id.");
        }
        return contextConfig.serviceAccount(serviceAccount);
    }

    public ContextConfig config(){
        ContextConfig contextConfig = ContextConfig.create();
        if (Objects.isNull(this.connectorContext)) return contextConfig;
        DataMap connectionConfig = this.connectorContext.getConnectionConfig();

        String serviceAccount = connectionConfig.getString("serviceAccount");
        if (null == serviceAccount || "".equals(serviceAccount)){
            throw new CoreException("Credentials is must not be null or not be empty.");
        }
        try {
            JSONObject parse = JSONUtil.parseObj(serviceAccount);
            Object projectId = parse.get("project_id");
            if (null == projectId ){
                throw new CoreException("Credentials is must not be null or not be empty.");
            }
            contextConfig.projectId(String.valueOf(projectId));
        }catch (Exception e){
            throw new CoreException("Credentials is must not be null or not be empty, can not get project id.");
        }


        String tableSet = connectionConfig.getString("tableSet");
        if (null == tableSet || "".equals(tableSet)){
            throw new CoreException("Credentials is must not be null or not be empty.");
        }
       DataMap nodeConfig = this.connectorContext.getNodeConfig();
       if (Objects.nonNull(nodeConfig)){
           String writeMode = nodeConfig.getString("writeMode");
           if (null == writeMode || "".equals(writeMode)){
               throw new CoreException("WriteMode is must not be null or not be empty.");
           }
           contextConfig.writeMode(writeMode);
           if ("MIXED_UPDATES".equals(writeMode)) {
               String cursorSchema = nodeConfig.getString("cursorSchema");
               if (null == cursorSchema || "".equals(cursorSchema)) {
                   throw new CoreException("WriteMode is MIXED_UPDATES and CursorSchema is must not be null or not be empty.");
               }
               contextConfig.cursorSchema(cursorSchema);
               String mergeDelay = nodeConfig.getString("mergeDelay");
               if (null == mergeDelay) {
                   throw new CoreException("WriteMode is MIXED_UPDATES and MergeDelay is must not be null or not be empty.");
               }
               contextConfig.mergeDelay(Long.valueOf(mergeDelay));
           }
       }



        return contextConfig.serviceAccount(serviceAccount).tableSet(tableSet);
    }
}
