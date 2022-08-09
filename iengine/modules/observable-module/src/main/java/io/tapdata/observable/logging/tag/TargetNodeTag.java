package io.tapdata.observable.logging.tag;

/**
 * @author Dexter
 */
public enum TargetNodeTag implements LogTag {
    NODE_TARGET_CONNECTION_STATUS_CHECK("Target Connection Status Check"),
    NODE_TARGET_DROP_TABLE("Target Drop Table"),
    NODE_TARGET_CREATE_TABLE("Target Create Table"),
    NODE_TARGET_CLEAR_TABLE("Target Clear Table"),
    NODE_TARGET_CREATE_INDEX("Target Create Index"),
    ;

    private final String tag;
    private TargetNodeTag(String tag) {
        this.tag = tag;
    }

    @Override
    public String getTag() {
        return tag;
    }
}
