package io.tapdata.bigquery.service.bigQuery;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.tapdata.bigquery.entity.ContextConfig;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;


public abstract class BigQueryStart {
    ContextConfig config;
    TapConnectionContext connectorContext;
    SqlMarker sqlMarker;
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
        if (null == this.connectorContext) return contextConfig;
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

        return contextConfig.serviceAccount(serviceAccount).tableSet(tableSet);
    }
}
