package io.tapdata.connector.tidb.util.pojo;

public class Changefeed {
    private Long startTs;
    private String sinkUri;
    private String changefeedId;

    public Long getStartTs() {
        return startTs;
    }

    public void setStarTs(Long startTs) {
        this.startTs = startTs;
    }

    public String getSinkUri() {
        return sinkUri;
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
