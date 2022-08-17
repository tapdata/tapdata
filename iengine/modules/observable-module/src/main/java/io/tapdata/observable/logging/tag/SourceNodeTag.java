package io.tapdata.observable.logging.tag;

/**
 * @author Dexter
 */
public enum SourceNodeTag implements LogTag {
    NODE_SOURCE_CONNECTION_STATUS_CHECK("Source Connection Status Check"),
    NODE_SOURCE_INITIAL_SYNC("Source Initial Sync"),
    NODE_SOURCE_INCREMENTAL_SYNC("Source Incremental Sync"),
    ;

    private final String tag;
    private SourceNodeTag(String tag) {
        this.tag = tag;
    }

    @Override
    public String getTag() {
        return tag;
    }
}
