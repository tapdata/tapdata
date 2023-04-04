package io.tapdata.connector.tidb.util.pojo;

public class Changefeed {
    private Long startTs;
    private String sinkUri;
    // synchronize DDL
    private Boolean syncDdl;
    private  String changefeedId;

    public String getChangefeedId() {
        return changefeedId;
    }
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


    public Changefeed() {
    }

    public void setChangeFeedId(String changeFeedId) {
        this.changefeedId = changeFeedId;
    }

    public Changefeed(String sinkUri, Boolean syncDdl, String changeFeedId, Boolean ignoreIneligibleTable) {
        this.sinkUri = sinkUri;
        this.syncDdl = syncDdl;
        this.changefeedId = changeFeedId;
        this.forceReplicate = forceReplicate;
    }
}
