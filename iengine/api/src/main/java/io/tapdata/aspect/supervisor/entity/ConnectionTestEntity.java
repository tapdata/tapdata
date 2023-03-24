package io.tapdata.aspect.supervisor.entity;

import io.tapdata.entity.utils.DataMap;

public class ConnectionTestEntity extends DisposableThreadGroupBase {
    String connectionId;
    String connectionName;
    String type;
    String databaseType;
    String pdkType;
    String pdkHash;
    String schemaVersion;
    String associateId;
    long time;

    public String getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(String connectionId) {
        this.connectionId = connectionId;
    }

    public ConnectionTestEntity connectionId(String connectionId) {
        this.connectionId = connectionId;
        return this;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public ConnectionTestEntity connectionName(String connectionName) {
        this.connectionName = connectionName;
        return this;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public ConnectionTestEntity type(String type) {
        this.type = type;
        return this;
    }

    public String getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    public ConnectionTestEntity databaseType(String databaseType) {
        this.databaseType = databaseType;
        return this;
    }

    public String getPdkType() {
        return pdkType;
    }

    public void setPdkType(String pdkType) {
        this.pdkType = pdkType;
    }

    public ConnectionTestEntity pdkType(String pdkType) {
        this.pdkType = pdkType;
        return this;
    }

    public String getPdkHash() {
        return pdkHash;
    }

    public void setPdkHash(String pdkHash) {
        this.pdkHash = pdkHash;
    }

    public ConnectionTestEntity pdkHash(String pdkHash) {
        this.pdkHash = pdkHash;
        return this;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public ConnectionTestEntity schemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
        return this;
    }
    public ConnectionTestEntity associateId(String associateId){
        this.associateId = associateId;
        return this;
    }
    public ConnectionTestEntity time(long time){
        this.time = time;
        return this;
    }

    @Override
    public DataMap summary() {
        return super.summary()
                .kv(MODE_KEY,"connection-test")
                .kv("databaseType",databaseType)
                .kv("name",connectionName)
                .kv("type",type)
                .kv("pdkType",pdkType)
                .kv("pdkHash",pdkHash)
                .kv("time",time)
                .kv("schemaVersion",schemaVersion)
                .kv("associateId",associateId);
    }
}
