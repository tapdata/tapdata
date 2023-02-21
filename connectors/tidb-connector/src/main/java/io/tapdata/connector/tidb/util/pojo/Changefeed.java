package io.tapdata.connector.tidb.util.pojo;

public class Changefeed {
    private Long startTs;
    private String sinkUri;
    private String changefeedId;
    // synchronize DDL
    private Boolean syncDdl;

    //Synchronize tables without valid indexes
    public  Boolean forceReplicate;

    public Long getStartTs() {
        return startTs;
    }

    public void setStartTs(Long startTs) {
        this.startTs = startTs;
    }

    public Boolean getSyncDdl() {
        return syncDdl;
    }

    public void setSyncDdl(Boolean syncDdl) {
        this.syncDdl = syncDdl;
    }

    public Boolean getForceReplicate() {
        return forceReplicate;
    }

    public void setForceReplicate(Boolean forceReplicate) {
        this.forceReplicate = forceReplicate;
    }

    public void setStarTs(Long startTs) {
        this.startTs = startTs;
    }

    public String getSinkUri() {
        return sinkUri;
    }
    //ignore Synchronize tables without primary keys
    private Boolean ignoreIneligibleTable;

    public Boolean getIgnoreIneligibleTable() {
        return ignoreIneligibleTable;
    }



    public void setIgnoreIneligibleTable(Boolean ignoreIneligibleTable) {
        this.ignoreIneligibleTable = ignoreIneligibleTable;
    }

    public void setSinkUri(String sinkUri) {
        this.sinkUri = sinkUri;
    }

    public String getChangefeedId() {
        return changefeedId;
    }

    public void setChangefeedId(String changefeedId) {
        this.changefeedId = changefeedId;
    }

    public Changefeed(Long startTs, String sinkUri, String changefeedId) {
        this.startTs = startTs;
        this.sinkUri = sinkUri;
        this.changefeedId = changefeedId;


    }

    public Changefeed() {
    }

    public Changefeed(String changefeedId) {
        this.changefeedId = changefeedId;
    }

}
