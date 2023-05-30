package io.tapdata.services;

import cn.hutool.core.thread.ThreadUtil;
import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.FormatUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.exception.ConnectionException;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.log.CollectLog;
import io.tapdata.modules.api.pdk.PDKUtils;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableUtil;
import io.tapdata.service.skeleton.annotation.RemoteService;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author GavinXiao
 * @description TestRunOfHttpReceiverService create by Gavin
 * @create 2023/5/26 9:56
 **/
@RemoteService
public class TestRunOfHttpReceiverService {

    private final Logger log = LogManager.getLogger(TestRunService.class);

    public static final String TAG = TestRunService.class.getSimpleName();

    @Data
    private static class Request {
//    private String connectionsId;


        /**
         * type of test run: source or target
         */
        private String type;

        /**
         * initial_sync
         * cdc
         * initial_sync+cdc
         */
        private String syncType;

        private int timeout;

        /**
         * connections
         */
        private Connections connections;

        private List<Map<String, Object>> params;

        private String version;


    }

    @Data
    @NoArgsConstructor
    private static class Response {

        private List<CollectLog.Log> logs;

        /**
         * target
         */
        private long insertedCount;
        private long removedCount;
        private long modifiedCount;

        private String version;

        public Response(List<CollectLog.Log> logs, String version) {
            this.logs = logs;
            this.version = version;
        }

        public void incrementInserted(long value) {
            this.insertedCount = this.insertedCount + value;
        }

        public void incrementModified(long value) {
            this.modifiedCount = this.modifiedCount + value;
        }

        public void incrementRemove(long value) {
            this.removedCount = this.removedCount + value;
        }
    }

    public Response testRun(final Request request) throws Throwable {
        long ts = System.currentTimeMillis();
        final CollectLog collectLog = getCollectLog();
        String version = request == null ? "" : request.getVersion();
        final Response response = new Response(collectLog.getLogs(), version);
        try {
            if (request == null) {
                throw new IllegalArgumentException("request param is null");
            }

            final Connections connections = request.getConnections();
            if (connections == null) {
                throw new IllegalArgumentException("connections is null");
            }
            final String pdkType = connections.getPdkType();
            if (StringUtils.isBlank(pdkType)) {
                throw new ConnectionException("Unknown connection pdk type");
            }
            String associateId = "TEST_RUN-" + connections.getName() + "_" + ts;
            collectLog.info("start {}...", associateId);

            Runnable runnable = () -> {
                Thread.currentThread().setName(associateId);
                ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
                DatabaseTypeEnum.DatabaseType databaseDefinition = ConnectionUtil.getDatabaseType(clientMongoOperator, connections.getPdkHash());
                if (databaseDefinition == null) {
                    collectLog.error(String.format("Unknown database type %s", connections.getDatabase_type()));
                    return;
                }

                PDKUtils pdkUtils = InstanceFactory.instance(PDKUtils.class);
                pdkUtils.downloadPdkFileIfNeed(connections.getPdkHash());

                ConnectorNode connectorNode = null;
                try {
                    String tableName = (String) connections.getConfig().get("collectionName");
                    TapTable tapTable = TapTableUtil.getTapTableByConnectionId(connections.getId(), tableName);
                    String nodeId = connections.getId();
                    TapTableMap<String, TapTable> tapTableMap = TapTableMap.create(nodeId, tapTable);
                    PdkTableMap pdkTableMap = new PdkTableMap(tapTableMap);
                    HazelcastInstance hazelcastInstance = HazelcastUtil.getInstance();
                    PdkStateMap pdkStateMap = new PdkStateMap(nodeId, hazelcastInstance);
                    PdkStateMap globalStateMap = PdkStateMap.globalStateMap(hazelcastInstance);

                    connectorNode = PDKIntegration.createConnectorBuilder()
                            .withDagId(String.valueOf(ts))
                            .withGroup(databaseDefinition.getGroup())
                            .withPdkId(databaseDefinition.getPdkId())
                            .withVersion(databaseDefinition.getVersion())
                            .withAssociateId(associateId)
                            .withConnectionConfig(DataMap.create(connections.getConfig()))
                            .withLog(collectLog)
                            .withTableMap(pdkTableMap)
                            .withStateMap(pdkStateMap)
                            .withGlobalStateMap(globalStateMap)
                            .build();

                    PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT, connectorNode::connectorInit, "Init PDK", TAG);

                    try {
                        TapConnectorContext connectorContext = connectorNode.getConnectorContext();
                        ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();

                        String type = request.getType();
                        if (StringUtils.equals(type, "source")) {
                            String syncType = request.getSyncType();
                            if (StringUtils.contains(syncType, "initial_sync")) {
                                // initial_sync
                                collectLog.info("Source starts to initial sync ");
                                BatchReadFunction batchReadFunction = connectorFunctions.getBatchReadFunction();
                                PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_BATCH_READ,
                                        () -> batchReadFunction.batchRead(connectorContext, tapTable, null, 512,
                                                (events, offsetObject) -> collectLog.info("initial_sync: {} {}", events, offsetObject)),
                                        TAG);
                                collectLog.info("initial sync end...");
                            }
                            if (StringUtils.contains(syncType, "cdc")) {
                                // cdc
                                collectLog.info("Source starts to incremental sync");
                                StreamReadFunction streamReadFunction = connectorFunctions.getStreamReadFunction();
                                PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_STREAM_READ,
                                        () -> streamReadFunction.streamRead(connectorContext, Collections.singletonList("tableName"), null, 512,
                                                StreamReadConsumer.create((events, offsetObject) -> collectLog.info("cdc: {} {}", events, offsetObject))),
                                        TAG);
                                collectLog.info("is complete");
                            }

                        } else {
                            collectLog.error("Unsupported type: {}, HttpReceiver connector only support source", type);
                        }
                    } finally {
                        PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, "Stop PDK", TAG);
                    }
                } finally {
                    if (connectorNode != null) {
                        connectorNode.unregisterMemoryFetcher();
                    }
                    PDKIntegration.releaseAssociateId(associateId);
                }
            };
            Future<?> future = ThreadUtil.execAsync(runnable);
            future.get(request.getTimeout(), TimeUnit.SECONDS);
            collectLog.info("test run end, cost {}ms", (System.currentTimeMillis() - ts));
        } catch (Throwable t) {
            collectLog.error("execute test run error: {}", t);
        }
        return response;
    }

    @NotNull
    private CollectLog getCollectLog() {
        return new CollectLog() {
            @Override
            public void debug(String message, Object... params) {
                super.debug(message, params);
                log.debug(FormatUtils.format(message, params));
            }

            @Override
            public void info(String message, Object... params) {
                super.info(message, params);
                log.info(FormatUtils.format(message, params));
            }

            @Override
            public void warn(String message, Object... params) {
                super.warn(message, params);
                log.warn(FormatUtils.format(message, params));
            }

            @Override
            public void error(String message, Object... params) {
                super.error(message, params);
                log.error(FormatUtils.format(message, params));
            }

            @Override
            public void error(String message, Throwable throwable) {
                super.error(message, throwable);
                log.error(message, throwable);
            }

            @Override
            public void fatal(String message, Object... params) {
                super.fatal(message, params);
                log.fatal(FormatUtils.format(message, params));
            }
        };
    }
}
