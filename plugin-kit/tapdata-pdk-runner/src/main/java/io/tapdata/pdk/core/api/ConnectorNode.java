package io.tapdata.pdk.core.api;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.common.MemoryFetcherFunctionV2;

import java.util.List;

public class ConnectorNode extends Node {
    private static final String TAG = ConnectorNode.class.getSimpleName();
    TapConnector connector;
    TapCodecsRegistry codecsRegistry;
    TapConnectorContext connectorContext;

    ConnectorFunctions connectorFunctions;
    TapCodecsFilterManager codecsFilterManager;
    String table;
    List<String> tables;

//    Queue<TapEvent> externalEvents;


    public void init(TapConnector tapNode, TapCodecsRegistry codecsRegistry, ConnectorFunctions connectorFunctions) {
        connector = tapNode;
        this.codecsRegistry = codecsRegistry;
        this.connectorFunctions = connectorFunctions;
        codecsFilterManager = new TapCodecsFilterManager(this.codecsRegistry);
//        externalEvents = new ConcurrentLinkedQueue<>();
    }

    public void init(TapConnector tapNode) {
        init(tapNode, new TapCodecsRegistry(), new ConnectorFunctions());
    }

//    public void offerExternalEvent(TapEvent tapEvent) {
//        if(externalEvents != null) {
//            if(tapEvent instanceof PatrolEvent) {
//                PatrolEvent patrolEvent = (PatrolEvent) tapEvent;
//                if(patrolEvent.applyState(getAssociateId(), PatrolEvent.STATE_ENTER)) {
//                    if(patrolEvent.getPatrolListener() != null) {
//                        CommonUtils.ignoreAnyError(() -> patrolEvent.getPatrolListener().patrol(getAssociateId(), PatrolEvent.STATE_ENTER), TAG);
//                    }
//                }
//            }
//            externalEvents.offer(tapEvent);
//        }
//    }

//    public List<TapEvent> pullAllExternalEventsInList(Consumer<TapEvent> consumer) {
//        if(externalEvents != null) {
//            if(externalEvents.isEmpty()) return null;
//
//            List<TapEvent> events = new ArrayList<>();
//            TapEvent tapEvent;
//            while((tapEvent = externalEvents.poll()) != null) {
//                if(consumer != null)
//                    consumer.accept(tapEvent);
//                events.add(tapEvent);
//            }
//            return events;
//        }
//        return null;
//    }

//    public void pullAllExternalEvents(Consumer<TapEvent> consumer) {
//        if(externalEvents != null) {
//            TapEvent tapEvent;
//            while((tapEvent = externalEvents.poll()) != null) {
//                consumer.accept(tapEvent);
//            }
//        }
//    }

    public TapCodecsRegistry getCodecsRegistry() {
        return codecsRegistry;
    }

    public void registerMemoryFetcher() {
        MemoryFetcherFunctionV2 memoryFetcherFunctionV2 = connectorFunctions.getMemoryFetcherFunctionV2();
        if(memoryFetcherFunctionV2 != null)
            PDKIntegration.registerMemoryFetcher(id() + "_" + associateId, memoryFetcherFunctionV2::memory);
    }

    public void unregisterMemoryFetcher() {
        PDKIntegration.unregisterMemoryFetcher(id() + "_" + associateId);
    }

    public void registerCapabilities() {
        connector.registerCapabilities(connectorFunctions, codecsRegistry);
    }

    public void connectorInit() throws Throwable {
        connector.init(connectorContext);
    }

    public void connectorStop() throws Throwable {
        try {
            connector.stop(connectorContext);
        } finally {
            unregisterMemoryFetcher();
        }
    }

    public TapConnectorContext getConnectorContext() {
        return connectorContext;
    }

    public TapConnector getConnector() {
        return connector;
    }

    public ConnectorFunctions getConnectorFunctions() {
        return connectorFunctions;
    }

    public TapCodecsFilterManager getCodecsFilterManager() {
        return codecsFilterManager;
    }

    public String getTable() {
        return table;
    }

    public List<String> getTables() {
        return tables;
    }
}
