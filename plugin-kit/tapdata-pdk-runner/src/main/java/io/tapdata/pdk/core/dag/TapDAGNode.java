package io.tapdata.pdk.core.dag;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TapDAGNode {
    protected DataMap nodeConfig;
    protected DataMap connectionConfig;
    protected String id;
    protected String pdkId;
    protected String group;
    protected String version;
    public static final String TYPE_TARGET = TapNodeInfo.NODE_TYPE_TARGET;
    public static final String TYPE_SOURCE = TapNodeInfo.NODE_TYPE_SOURCE;
    public static final String TYPE_PROCESSOR = TapNodeInfo.NODE_TYPE_PROCESSOR;
    public static final String TYPE_SOURCE_TARGET = TapNodeInfo.NODE_TYPE_SOURCE_TARGET;
    protected String type;
    /**
     * ["*"] means all tables in source; accept any table from source in target.
     */
    protected List<String> tables;
    protected String table;
    protected List<Map<String, Object>> tasks;
    protected List<String> parentNodeIds;
    protected List<String> childNodeIds;

    @Override
    public String toString() {
        return type + " " + id + ": " + (tables != null ? Arrays.toString(tables.toArray()) + " on " : table + " on ") + pdkId + "@" + group + "-v" + version;
    }

    public String verify() {
        if(id == null)
            return "missing id";
        if(pdkId == null)
            return "missing pdkId";
        if(group == null)
            return "missing group";
        if(type == null)
            return "missing type";
        if(version == null)
            return "missing version";
        if(!type.equals(TYPE_PROCESSOR) && ((tables == null || tables.isEmpty()) && table == null))
            return "missing tables or table";
        return null;
    }

    public List<Map<String, Object>> getTasks() {
        return tasks;
    }

    public void setTasks(List<Map<String, Object>> tasks) {
        this.tasks = tasks;
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getPdkId() {
        return pdkId;
    }

    public void setPdkId(String pdkId) {
        this.pdkId = pdkId;
    }

    public List<String> getParentNodeIds() {
        return parentNodeIds;
    }

    public void setParentNodeIds(List<String> parentNodeIds) {
        this.parentNodeIds = parentNodeIds;
    }

    public List<String> getChildNodeIds() {
        return childNodeIds;
    }

    public void setChildNodeIds(List<String> childNodeIds) {
        this.childNodeIds = childNodeIds;
    }

    public DataMap getNodeConfig() {
        return nodeConfig;
    }

    public void setNodeConfig(DataMap nodeConfig) {
        this.nodeConfig = nodeConfig;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public DataMap getConnectionConfig() {
        return connectionConfig;
    }

    public void setConnectionConfig(DataMap connectionConfig) {
        this.connectionConfig = connectionConfig;
    }
}
