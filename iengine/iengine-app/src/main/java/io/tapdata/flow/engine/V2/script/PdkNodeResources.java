package io.tapdata.flow.engine.V2.script;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.TapTableMap;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns the PDK resources shared by script and file-storage executors.
 *
 * <p>Closing is deliberately idempotent because executor caches can race with
 * explicit task shutdown. The script variant asks the holder to stop its
 * initialized connector before releasing the common maps; the file-storage
 * variant only releases the common resources because its connector node is
 * used as a class-loader provider.</p>
 */
public final class PdkNodeResources implements AutoCloseable {

    @FunctionalInterface
    interface CleanupAction {
        void run() throws Throwable;
    }

    private final String tag;
    private final String associateId;
    private final PdkStateMap stateMap;
    private final TapTableMap<String, TapTable> tapTableMap;
    private final Log logger;
    private final boolean stopConnector;
    private final CleanupAction stateReset;
    private final CleanupAction associateRelease;
    private final CleanupAction tableReset;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile ConnectorNode connectorNode;

    public PdkNodeResources(String tag,
                            String associateId,
                            PdkStateMap stateMap,
                            TapTableMap<String, TapTable> tapTableMap,
                            Log logger,
                            boolean stopConnector) {
        this(tag, associateId, stateMap, tapTableMap, logger, stopConnector,
                () -> {
                    if (stateMap != null) {
                        stateMap.reset();
                    }
                },
                () -> {
                    if (associateId != null) {
                        PDKIntegration.releaseAssociateId(associateId);
                    }
                },
                () -> {
                    if (tapTableMap != null) {
                        tapTableMap.reset();
                    }
                });
    }

    public PdkNodeResources(String associateId,
                            Runnable stateReset,
                            Runnable associateRelease,
                            Runnable tableReset) {
        this(null, associateId, null, null, null, false,
                stateReset::run, associateRelease::run, tableReset::run);
    }

    private PdkNodeResources(String tag,
                             String associateId,
                             PdkStateMap stateMap,
                             TapTableMap<String, TapTable> tapTableMap,
                             Log logger,
                             boolean stopConnector,
                             CleanupAction stateReset,
                             CleanupAction associateRelease,
                             CleanupAction tableReset) {
        this.tag = tag;
        this.associateId = associateId;
        this.stateMap = stateMap;
        this.tapTableMap = tapTableMap;
        this.logger = logger;
        this.stopConnector = stopConnector;
        this.stateReset = stateReset;
        this.associateRelease = associateRelease;
        this.tableReset = tableReset;
    }

    public void setConnectorNode(ConnectorNode connectorNode) {
        this.connectorNode = connectorNode;
    }

    public ConnectorNode getConnectorNode() {
        return connectorNode;
    }

    public String getAssociateId() {
        return associateId;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (stopConnector) {
            runSafely(() -> {
                ConnectorNode node = connectorNode;
                if (node != null) {
                    PDKInvocationMonitor.stop(node);
                    PDKInvocationMonitor.invoke(node, PDKMethod.STOP, node::connectorStop, tag);
                }
            }, "Stop PDK connector node failed");
        }
        if (stopConnector) {
            runSafely(stateReset, "Reset PDK state failed");
            runSafely(associateRelease, "Release PDK associate failed");
        } else {
            runSafely(associateRelease, "Release PDK associate failed");
            runSafely(stateReset, "Reset PDK state failed");
        }
        runSafely(tableReset, "Reset PDK table map failed");
    }

    private void runSafely(CleanupAction action, String message) {
        try {
            action.run();
        } catch (Throwable throwable) {
            if (logger != null) {
                logger.warn(message + ": " + throwable.getMessage());
            }
        }
    }
}
