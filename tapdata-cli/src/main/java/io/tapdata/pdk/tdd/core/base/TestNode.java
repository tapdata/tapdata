package io.tapdata.pdk.tdd.core.base;

import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;

public class TestNode {
    private ConnectorNode connectorNode;
    private RecordEventExecute recordEventExecute;
    private TapNodeInfo nodeInfo;

    public TestNode(TapNodeInfo nodeInfo, ConnectorNode connectorNode, RecordEventExecute recordEventExecute) {
        this.nodeInfo = nodeInfo;
        this.connectorNode = connectorNode;
        this.recordEventExecute = recordEventExecute;
    }

    public ConnectorNode connectorNode() {
        return this.connectorNode;
    }

    public RecordEventExecute recordEventExecute() {
        return this.recordEventExecute;
    }

    public TapNodeInfo nodeInfo() {
        return this.nodeInfo;
    }
}