package io.tapdata.pdk.core.tapnode;

import io.tapdata.pdk.apis.TapNode;

public class TapNodeInstance {
    private TapNodeInfo tapNodeInfo;
    private TapNode tapNode;

    public TapNodeInfo getTapNodeInfo() {
        return tapNodeInfo;
    }

    public void setTapNodeInfo(TapNodeInfo tapNodeInfo) {
        this.tapNodeInfo = tapNodeInfo;
    }

    public TapNode getTapNode() {
        return tapNode;
    }

    public void setTapNode(TapNode tapNode) {
        this.tapNode = tapNode;
    }
}
