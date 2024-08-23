package io.tapdata.dao;

import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;

public class DoSnapshotFunctions {
    private ConnectorNode connectorNode;
    private BatchCountFunction batchCountFunction;
    private BatchReadFunction batchReadFunction;
    private QueryByAdvanceFilterFunction queryByAdvanceFilterFunction;
    private ExecuteCommandFunction executeCommandFunction;

    public DoSnapshotFunctions(ConnectorNode connectorNode, BatchCountFunction batchCountFunction, BatchReadFunction batchReadFunction, QueryByAdvanceFilterFunction queryByAdvanceFilterFunction, ExecuteCommandFunction executeCommandFunction) {
        this.connectorNode = connectorNode;
        this.batchCountFunction = batchCountFunction;
        this.batchReadFunction = batchReadFunction;
        this.queryByAdvanceFilterFunction = queryByAdvanceFilterFunction;
        this.executeCommandFunction = executeCommandFunction;
    }

    public ConnectorNode getConnectorNode() {
        return connectorNode;
    }

    public BatchCountFunction getBatchCountFunction() {
        return batchCountFunction;
    }

    public BatchReadFunction getBatchReadFunction() {
        return batchReadFunction;
    }

    public QueryByAdvanceFilterFunction getQueryByAdvanceFilterFunction() {
        return queryByAdvanceFilterFunction;
    }

    public ExecuteCommandFunction getExecuteCommandFunction() {
        return executeCommandFunction;
    }
}
