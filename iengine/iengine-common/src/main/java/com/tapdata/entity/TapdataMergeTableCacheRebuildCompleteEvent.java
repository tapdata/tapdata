package com.tapdata.entity;


public class TapdataMergeTableCacheRebuildCompleteEvent extends TapdataEvent{
    private static final long serialVersionUID = 4717006177280282597L;

    public String getMergeTablePropertiesId() {
        return mergeTablePropertiesId;
    }

    public void setMergeTablePropertiesId(String mergeTablePropertiesId) {
        this.mergeTablePropertiesId = mergeTablePropertiesId;
    }

    private String mergeTablePropertiesId;

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    private String nodeId;
    public TapdataMergeTableCacheRebuildCompleteEvent(String mergeTablePropertiesId, String nodeId) {
        this.mergeTablePropertiesId = mergeTablePropertiesId;
        this.nodeId = nodeId;
    }

}
