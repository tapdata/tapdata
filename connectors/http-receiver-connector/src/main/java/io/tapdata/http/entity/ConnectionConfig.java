package io.tapdata.http.entity;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;

/**
 * @author GavinXiao
 * @description ConnectionConfig create by Gavin
 * @create 2023/5/17 17:20
 **/
public class ConnectionConfig {
    private String tableName;
    private String hookUrl;
    private String script;

    public static ConnectionConfig create(TapConnectionContext context){
        DataMap config = context.getConnectionConfig();
        return new ConnectionConfig()
                .tableName(config.getString("tableName"))
                .hookUrl(config.getString("hookText"))
                .script(config.getString("eventScript"));
    }

    public ConnectionConfig tableName(String tableName){
        this.tableName = tableName;
        return this;
    }

    public String tableName(){
        return this.tableName;
    }

    public ConnectionConfig hookUrl(String hookUrl){
        this.hookUrl = hookUrl;
        return this;
    }

    public String hookUrl(){
        return this.hookUrl;
    }


    public ConnectionConfig script(String script){
        this.script = script;
        return this;
    }

    public String script(){
        return this.script;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getHookUrl() {
        return hookUrl;
    }

    public void setHookUrl(String hookUrl) {
        this.hookUrl = hookUrl;
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }
}
