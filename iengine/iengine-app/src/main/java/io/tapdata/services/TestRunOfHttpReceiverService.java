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

import static io.tapdata.entity.simplify.TapSimplify.fromJsonArray;

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

    public List<Map<String, Object>> testData(final String connectionId) {
        return testData(connectionId, 1);
    }

    public List<Map<String, Object>> testData(final String connectionId, final Integer dataCount) {
        return (List<Map<String, Object>>) fromJsonArray(json);
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

    private String json = "[\n" +
            "  {\n" +
            "    \"action\": \"update_custom_field\",\n" +
            "    \"event\": \"ISSUE_UPDATED\",\n" +
            "    \"eventName\": \"更新事项信息\",\n" +
            "    \"sender\": {\n" +
            "        \"id\": 8724348,\n" +
            "        \"login\": \"znRedQSeqW\",\n" +
            "        \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/d65a3000-06d6-4613-872b-0bd470e752ce.png?imageView2/1/w/0/h/0\",\n" +
            "        \"url\": \"https://tapdata.coding.net/api/user/key/znRedQSeqW\",\n" +
            "        \"html_url\": \"https://tapdata.coding.net/u/znRedQSeqW\",\n" +
            "        \"name\": \"HardyZhu\",\n" +
            "        \"name_pinyin\": \"HardyZhu\"\n" +
            "    },\n" +
            "    \"project\": {\n" +
            "        \"id\": 342870,\n" +
            "        \"icon\": \"https://dn-coding-net-production-pp.codehub.cn/79a8bcc4-d9cc-4061-940d-5b3bb31bf571.png\",\n" +
            "        \"url\": \"https://tapdata.coding.net/p/tapdata\",\n" +
            "        \"description\": \"Tapdata DaaS \",\n" +
            "        \"name\": \"tapdata\",\n" +
            "        \"display_name\": \"Tapdata DaaS\"\n" +
            "    },\n" +
            "    \"team\": {\n" +
            "        \"id\": 155077,\n" +
            "        \"domain\": \"tapdata\",\n" +
            "        \"url\": \"https://tapdata.coding.net\",\n" +
            "        \"introduction\": \"\",\n" +
            "        \"name\": \"Tapdata\",\n" +
            "        \"name_pinyin\": \"Tapdata\",\n" +
            "        \"avatar\": \"https://coding-net-production-pp-ci.codehub.cn/9837b4a6-442b-4513-b51d-a2030c4a6ede.png\"\n" +
            "    },\n" +
            "    \"defect\": {\n" +
            "        \"html_url\": \"https://tapdata.coding.net/p/tapdata/bug-tracking/issues/145053/detail\",\n" +
            "        \"fieldInfos\": [\n" +
            "            {\n" +
            "                \"id\": 37615925,\n" +
            "                \"name\": \"目标版本\",\n" +
            "                \"type\": \"SELECT_MULTI\",\n" +
            "                \"newFieldValue\": \"[627439]\"\n" +
            "            }\n" +
            "        ],\n" +
            "        \"typeZh\": \"缺陷\",\n" +
            "        \"statusZh\": \"进行中\",\n" +
            "        \"type\": \"DEFECT\",\n" +
            "        \"project_id\": 342870,\n" +
            "        \"code\": 145053,\n" +
            "        \"parent_code\": 0,\n" +
            "        \"title\": \"复制任务看不到模型，节点也不显示已选择的表\",\n" +
            "        \"creator\": {\n" +
            "            \"id\": 7959788,\n" +
            "            \"login\": \"vVRqbLRQvj\",\n" +
            "            \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-fPQSaloJAwpWXAjMwPQg.jpg\",\n" +
            "            \"url\": \"https://tapdata.coding.net/api/user/key/vVRqbLRQvj\",\n" +
            "            \"html_url\": \"https://tapdata.coding.net/u/vVRqbLRQvj\",\n" +
            "            \"name\": \"Jason\",\n" +
            "            \"name_pinyin\": \"Jason\",\n" +
            "            \"email\": \"\",\n" +
            "            \"phone\": \"\"\n" +
            "        },\n" +
            "        \"status\": \"处理中\",\n" +
            "        \"assignee\": {\n" +
            "            \"id\": 8724348,\n" +
            "            \"login\": \"znRedQSeqW\",\n" +
            "            \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/d65a3000-06d6-4613-872b-0bd470e752ce.png?imageView2/1/w/0/h/0\",\n" +
            "            \"url\": \"https://tapdata.coding.net/api/user/key/znRedQSeqW\",\n" +
            "            \"html_url\": \"https://tapdata.coding.net/u/znRedQSeqW\",\n" +
            "            \"name\": \"HardyZhu\",\n" +
            "            \"name_pinyin\": \"HardyZhu\",\n" +
            "            \"email\": \"\",\n" +
            "            \"phone\": \"\"\n" +
            "        },\n" +
            "        \"priority\": 3,\n" +
            "        \"iteration\": {\n" +
            "            \"title\": \"sprint #67\",\n" +
            "            \"goal\": \"\",\n" +
            "            \"html_url\": \"https://tapdata.coding.net/p/tapdata/iterations/136626\",\n" +
            "            \"project_id\": 342870,\n" +
            "            \"code\": 136626,\n" +
            "            \"creator\": {\n" +
            "                \"id\": 8136202,\n" +
            "                \"login\": \"EKPSyphfdZ\",\n" +
            "                \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-AUxzbbMIyLfUtPQGZspa.jpg\",\n" +
            "                \"url\": \"https://tapdata.coding.net/api/user/key/EKPSyphfdZ\",\n" +
            "                \"html_url\": \"https://tapdata.coding.net/u/EKPSyphfdZ\",\n" +
            "                \"name\": \"Martin\",\n" +
            "                \"name_pinyin\": \"Martin\",\n" +
            "                \"email\": \"\",\n" +
            "                \"phone\": \"\"\n" +
            "            },\n" +
            "            \"watchers\": [],\n" +
            "            \"status\": \"PROCESSING\",\n" +
            "            \"plan_issue_number\": 295,\n" +
            "            \"start_at\": 1683475200000,\n" +
            "            \"end_at\": 1685721599000,\n" +
            "            \"created_at\": 1678762603000,\n" +
            "            \"updated_at\": 1684312697000\n" +
            "        },\n" +
            "        \"description\": \"\\n![image.png](https://coding-net-production-file-ci.codehub.cn/knowledge%2Fc6337771-a84c-4806-9144-bf6471c06123.png?sign=Y9mQnU86G04uwcxs0%2FPP0S2Jb5RhPTEyNTcyNDI1OTkmaz1BS0lERkk1Y3NzYjRIRHNHWWFLbzB2WnczdVVucWQ5TksxU2ImZT0xNjg1MTAyNDEyJnQ9MTY4NTEwMDYxMiZyPTY5OTAwMTAxJmY9LzEyNTcyNDI1OTkvY29kaW5nLW5ldC1wcm9kdWN0aW9uLWZpbGUva25vd2xlZGdlL2M2MzM3NzcxLWE4NGMtNDgwNi05MTQ0LWJmNjQ3MWMwNjEyMy5wbmcmYj1jb2RpbmctbmV0LXByb2R1Y3Rpb24tZmlsZQ%3D%3D)\\n\\n![image.png](https://coding-net-production-file-ci.codehub.cn/knowledge%2Fb7dfa45f-668f-449b-8e56-a9a668e557ae.png?sign=bT%2B2E%2BGXuhAE5CXnEKltcn%2FRBEFhPTEyNTcyNDI1OTkmaz1BS0lERkk1Y3NzYjRIRHNHWWFLbzB2WnczdVVucWQ5TksxU2ImZT0xNjg1MTAyNDEyJnQ9MTY4NTEwMDYxMiZyPTQ1NTIzNDQ5JmY9LzEyNTcyNDI1OTkvY29kaW5nLW5ldC1wcm9kdWN0aW9uLWZpbGUva25vd2xlZGdlL2I3ZGZhNDVmLTY2OGYtNDQ5Yi04ZTU2LWE5YTY2OGU1NTdhZS5wbmcmYj1jb2RpbmctbmV0LXByb2R1Y3Rpb24tZmlsZQ%3D%3D)\\n\\n![image.png](https://coding-net-production-file-ci.codehub.cn/knowledge%2F3a7e675d-bda3-428c-b78f-7140bf1f978d.png?sign=%2FbgN%2FRIk4ckJ%2Bdr8ebciCcbto5JhPTEyNTcyNDI1OTkmaz1BS0lERkk1Y3NzYjRIRHNHWWFLbzB2WnczdVVucWQ5TksxU2ImZT0xNjg1MTAyNDEyJnQ9MTY4NTEwMDYxMiZyPTU1NjcwOTA4JmY9LzEyNTcyNDI1OTkvY29kaW5nLW5ldC1wcm9kdWN0aW9uLWZpbGUva25vd2xlZGdlLzNhN2U2NzVkLWJkYTMtNDI4Yy1iNzhmLTcxNDBiZjFmOTc4ZC5wbmcmYj1jb2RpbmctbmV0LXByb2R1Y3Rpb24tZmlsZQ%3D%3D)\\n\\n\\n\\n按照模块划分 | 按照产生阶段（产品、研发设计、研发实现、测试、交付） | 是否常见场景/边角场景 | 是否同构数据库迁移场景 | Cloud vs OP | 是否外部数据源问题 | 数据层面的错误类型 | 是否性能问题 | 是否引擎框架问题 | 是否系统配置相关 | 2.x版本是否规避\\n:----------- | :----------- | :----------- | :----------- | :----------- | :----------- | :----------- | :----------- | :----------- | :----------- | :-----------\\nA | A | A | A | A | A | A | A | A | A | A\\n\\n\",\n" +
            "        \"created_at\": 1684990040000,\n" +
            "        \"updated_at\": 1685100612326,\n" +
            "        \"issue_status\": {\n" +
            "            \"id\": 1587663,\n" +
            "            \"name\": \"处理中\",\n" +
            "            \"type\": \"PROCESSING\"\n" +
            "        },\n" +
            "        \"watchers\": [\n" +
            "            {\n" +
            "                \"id\": 7959788,\n" +
            "                \"login\": \"vVRqbLRQvj\",\n" +
            "                \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-fPQSaloJAwpWXAjMwPQg.jpg\",\n" +
            "                \"url\": \"\",\n" +
            "                \"html_url\": \"\",\n" +
            "                \"name\": \"Jason\",\n" +
            "                \"name_pinyin\": \"Jason\",\n" +
            "                \"email\": \"\",\n" +
            "                \"phone\": \"\"\n" +
            "            }\n" +
            "        ],\n" +
            "        \"labels\": []\n" +
            "    },\n" +
            "    \"issue\": {\n" +
            "        \"html_url\": \"https://tapdata.coding.net/p/tapdata/bug-tracking/issues/145053/detail\",\n" +
            "        \"fieldInfos\": [\n" +
            "            {\n" +
            "                \"id\": 37615925,\n" +
            "                \"name\": \"目标版本\",\n" +
            "                \"type\": \"SELECT_MULTI\",\n" +
            "                \"newFieldValue\": \"[627439]\"\n" +
            "            }\n" +
            "        ],\n" +
            "        \"typeZh\": \"缺陷\",\n" +
            "        \"statusZh\": \"进行中\",\n" +
            "        \"type\": \"DEFECT\",\n" +
            "        \"project_id\": 342870,\n" +
            "        \"code\": 145053,\n" +
            "        \"parent_code\": 0,\n" +
            "        \"title\": \"复制任务看不到模型，节点也不显示已选择的表\",\n" +
            "        \"creator\": {\n" +
            "            \"id\": 7959788,\n" +
            "            \"login\": \"vVRqbLRQvj\",\n" +
            "            \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-fPQSaloJAwpWXAjMwPQg.jpg\",\n" +
            "            \"url\": \"https://tapdata.coding.net/api/user/key/vVRqbLRQvj\",\n" +
            "            \"html_url\": \"https://tapdata.coding.net/u/vVRqbLRQvj\",\n" +
            "            \"name\": \"Jason\",\n" +
            "            \"name_pinyin\": \"Jason\",\n" +
            "            \"email\": \"\",\n" +
            "            \"phone\": \"\"\n" +
            "        },\n" +
            "        \"status\": \"处理中\",\n" +
            "        \"assignee\": {\n" +
            "            \"id\": 8724348,\n" +
            "            \"login\": \"znRedQSeqW\",\n" +
            "            \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/d65a3000-06d6-4613-872b-0bd470e752ce.png?imageView2/1/w/0/h/0\",\n" +
            "            \"url\": \"https://tapdata.coding.net/api/user/key/znRedQSeqW\",\n" +
            "            \"html_url\": \"https://tapdata.coding.net/u/znRedQSeqW\",\n" +
            "            \"name\": \"HardyZhu\",\n" +
            "            \"name_pinyin\": \"HardyZhu\",\n" +
            "            \"email\": \"\",\n" +
            "            \"phone\": \"\"\n" +
            "        },\n" +
            "        \"priority\": 3,\n" +
            "        \"iteration\": {\n" +
            "            \"title\": \"sprint #67\",\n" +
            "            \"goal\": \"\",\n" +
            "            \"html_url\": \"https://tapdata.coding.net/p/tapdata/iterations/136626\",\n" +
            "            \"project_id\": 342870,\n" +
            "            \"code\": 136626,\n" +
            "            \"creator\": {\n" +
            "                \"id\": 8136202,\n" +
            "                \"login\": \"EKPSyphfdZ\",\n" +
            "                \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-AUxzbbMIyLfUtPQGZspa.jpg\",\n" +
            "                \"url\": \"https://tapdata.coding.net/api/user/key/EKPSyphfdZ\",\n" +
            "                \"html_url\": \"https://tapdata.coding.net/u/EKPSyphfdZ\",\n" +
            "                \"name\": \"Martin\",\n" +
            "                \"name_pinyin\": \"Martin\",\n" +
            "                \"email\": \"\",\n" +
            "                \"phone\": \"\"\n" +
            "            },\n" +
            "            \"watchers\": [],\n" +
            "            \"status\": \"PROCESSING\",\n" +
            "            \"plan_issue_number\": 295,\n" +
            "            \"start_at\": 1683475200000,\n" +
            "            \"end_at\": 1685721599000,\n" +
            "            \"created_at\": 1678762603000,\n" +
            "            \"updated_at\": 1684312697000\n" +
            "        },\n" +
            "        \"description\": \"\\n![image.png](https://coding-net-production-file-ci.codehub.cn/knowledge%2Fc6337771-a84c-4806-9144-bf6471c06123.png?sign=Y9mQnU86G04uwcxs0%2FPP0S2Jb5RhPTEyNTcyNDI1OTkmaz1BS0lERkk1Y3NzYjRIRHNHWWFLbzB2WnczdVVucWQ5TksxU2ImZT0xNjg1MTAyNDEyJnQ9MTY4NTEwMDYxMiZyPTY5OTAwMTAxJmY9LzEyNTcyNDI1OTkvY29kaW5nLW5ldC1wcm9kdWN0aW9uLWZpbGUva25vd2xlZGdlL2M2MzM3NzcxLWE4NGMtNDgwNi05MTQ0LWJmNjQ3MWMwNjEyMy5wbmcmYj1jb2RpbmctbmV0LXByb2R1Y3Rpb24tZmlsZQ%3D%3D)\\n\\n![image.png](https://coding-net-production-file-ci.codehub.cn/knowledge%2Fb7dfa45f-668f-449b-8e56-a9a668e557ae.png?sign=bT%2B2E%2BGXuhAE5CXnEKltcn%2FRBEFhPTEyNTcyNDI1OTkmaz1BS0lERkk1Y3NzYjRIRHNHWWFLbzB2WnczdVVucWQ5TksxU2ImZT0xNjg1MTAyNDEyJnQ9MTY4NTEwMDYxMiZyPTQ1NTIzNDQ5JmY9LzEyNTcyNDI1OTkvY29kaW5nLW5ldC1wcm9kdWN0aW9uLWZpbGUva25vd2xlZGdlL2I3ZGZhNDVmLTY2OGYtNDQ5Yi04ZTU2LWE5YTY2OGU1NTdhZS5wbmcmYj1jb2RpbmctbmV0LXByb2R1Y3Rpb24tZmlsZQ%3D%3D)\\n\\n![image.png](https://coding-net-production-file-ci.codehub.cn/knowledge%2F3a7e675d-bda3-428c-b78f-7140bf1f978d.png?sign=%2FbgN%2FRIk4ckJ%2Bdr8ebciCcbto5JhPTEyNTcyNDI1OTkmaz1BS0lERkk1Y3NzYjRIRHNHWWFLbzB2WnczdVVucWQ5TksxU2ImZT0xNjg1MTAyNDEyJnQ9MTY4NTEwMDYxMiZyPTU1NjcwOTA4JmY9LzEyNTcyNDI1OTkvY29kaW5nLW5ldC1wcm9kdWN0aW9uLWZpbGUva25vd2xlZGdlLzNhN2U2NzVkLWJkYTMtNDI4Yy1iNzhmLTcxNDBiZjFmOTc4ZC5wbmcmYj1jb2RpbmctbmV0LXByb2R1Y3Rpb24tZmlsZQ%3D%3D)\\n\\n\\n\\n按照模块划分 | 按照产生阶段（产品、研发设计、研发实现、测试、交付） | 是否常见场景/边角场景 | 是否同构数据库迁移场景 | Cloud vs OP | 是否外部数据源问题 | 数据层面的错误类型 | 是否性能问题 | 是否引擎框架问题 | 是否系统配置相关 | 2.x版本是否规避\\n:----------- | :----------- | :----------- | :----------- | :----------- | :----------- | :----------- | :----------- | :----------- | :----------- | :-----------\\nA | A | A | A | A | A | A | A | A | A | A\\n\\n\",\n" +
            "        \"created_at\": 1684990040000,\n" +
            "        \"updated_at\": 1685100612326,\n" +
            "        \"issue_status\": {\n" +
            "            \"id\": 1587663,\n" +
            "            \"name\": \"处理中\",\n" +
            "            \"type\": \"PROCESSING\"\n" +
            "        },\n" +
            "        \"watchers\": [\n" +
            "            {\n" +
            "                \"id\": 7959788,\n" +
            "                \"login\": \"vVRqbLRQvj\",\n" +
            "                \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-fPQSaloJAwpWXAjMwPQg.jpg\",\n" +
            "                \"url\": \"\",\n" +
            "                \"html_url\": \"\",\n" +
            "                \"name\": \"Jason\",\n" +
            "                \"name_pinyin\": \"Jason\",\n" +
            "                \"email\": \"\",\n" +
            "                \"phone\": \"\"\n" +
            "            }\n" +
            "        ],\n" +
            "        \"labels\": []\n" +
            "    },\n" +
            "    \"hook\": {\n" +
            "        \"id\": \"e61e0165-86a3-4223-9241-5dc6bc8c43a0\",\n" +
            "        \"name\": \"web\",\n" +
            "        \"type\": \"Repository\",\n" +
            "        \"active\": true,\n" +
            "        \"events\": [\n" +
            "            \"ISSUE_UPDATED\",\n" +
            "            \"ISSUE_CREATED\",\n" +
            "            \"ISSUE_STATUS_UPDATED\",\n" +
            "            \"ISSUE_ITERATION_CHANGED\",\n" +
            "            \"ISSUE_DELETED\",\n" +
            "            \"ISSUE_COMMENT_CREATED\",\n" +
            "            \"ISSUE_RELATIONSHIP_CHANGED\",\n" +
            "            \"ISSUE_HOUR_RECORD_UPDATED\",\n" +
            "            \"ISSUE_ASSIGNEE_CHANGED\"\n" +
            "        ],\n" +
            "        \"config\": {\n" +
            "            \"content_type\": \"application/json\",\n" +
            "            \"url\": \"http://139.198.105.8:32361/api/proxy/callback/zJB1SiyQScDtldUPhWfIl7WpLp2BbF4KUQ==\"\n" +
            "        },\n" +
            "        \"created_at\": 1684402135000,\n" +
            "        \"updated_at\": 1685100247000\n" +
            "    },\n" +
            "    \"hook_id\": \"e61e0165-86a3-4223-9241-5dc6bc8c43a0\"\n" +
            "}\n" +
            "]";
}
