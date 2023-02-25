package io.tapdata.bigquery.entity;

public class ContextConfig {
    private String serviceAccount;
    private String projectId;
    private String tableSet ;
    public static ContextConfig create(){
        return new ContextConfig();
    }

    public ContextConfig serviceAccount(String serviceAccount){
        this.serviceAccount= serviceAccount;
        return this;
    }
    public String serviceAccount(){
        return this.serviceAccount;
    }
    public ContextConfig projectId(String projectId){
        this.projectId= projectId;
        return this;
    }
    public String projectId(){
        return this.projectId;
    }
    public ContextConfig tableSet(String tableSet){
        this.tableSet= tableSet;
        return this;
    }
    public String tableSet(){
        return this.tableSet;
    }
}
