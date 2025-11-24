package io.tapdata.websocket.handler;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.alarm.AlarmComponentEnum;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.alarm.AlarmTypeEnum;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.TaskOpRespDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmDatasourceDto;
import io.tapdata.aspect.supervisor.AspectRunnableUtil;
import io.tapdata.aspect.supervisor.DisposableThreadGroupAspect;
import io.tapdata.aspect.supervisor.entity.ConnectionTestEntity;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.ReflectionUtil;
import io.tapdata.exception.ConnectionException;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.ExecuteCommandV2Function;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.threadgroup.DisposableThreadGroup;
import io.tapdata.threadgroup.utils.DisposableType;
import io.tapdata.utils.ConnectionUpdateOperation;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventHandler;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

@EventHandlerAnnotation(type = "datasourceMonitor")
public class DatasourceMonitorHandler implements WebSocketEventHandler {

    private static final Logger logger = LogManager.getLogger(TestConnectionHandler.class);
    private static final String TAG = DatasourceMonitorHandler.class.getSimpleName();
    private final static String MONITOR_API = "monitorAPI";
    private static final String IS_DELETED = "is_deleted";
    private static final String DAG_NODES_CONNECTION_ID = "dag.nodes.connectionId";
    private static final String STATUS = "status";
    private static final String STATUS_RUNNING = "running";


    static Logger getLogger() {
        return logger;
    }

    private ClientMongoOperator clientMongoOperator;

    @Override
    public void initialize(ClientMongoOperator clientMongoOperator) {
        this.clientMongoOperator = clientMongoOperator;
    }

    @Override
    public Object handle(Map event, SendMessage sendMessage) {
        Connections connections = parseConnection(event);
        String connectionId = ConnectionUpdateOperation.ID.getString(event);
        String connName = ConnectionUpdateOperation.NAME.getOrDefault(event, "");
        String pdkHash = ConnectionUpdateOperation.PDK_HASH.getOrDefault(event, "");

        ConnectionTestEntity entity = new ConnectionTestEntity()
                .associateId(UUID.randomUUID().toString())
                .time(System.nanoTime())
                .connectionId(connectionId)
                .type(ConnectionUpdateOperation.TYPE.getString(event))
                .connectionName(connName)
                .pdkType(ConnectionUpdateOperation.PDK_TYPE.getString(event))
                .pdkHash(pdkHash)
                .schemaVersion(ConnectionUpdateOperation.SCHEMA_VERSION.getString(event))
                .databaseType(ConnectionUpdateOperation.DATABASE_TYPE.getString(event));

        String threadName = String.format("TEST-CONNECTION-%s", connName);
        DisposableThreadGroup threadGroup = new DisposableThreadGroup(DisposableType.CONNECTION_TEST, threadName);

        AspectRunnableUtil.aspectAndStart(new DisposableThreadGroupAspect<>(connectionId, threadGroup, entity)
                , () -> handleSync(connections)
        );
        return null;
    }

    protected void handleSync(Connections connections) {
        DatabaseTypeEnum.DatabaseType databaseType = getDatabaseType(connections);
        long ts = System.currentTimeMillis();
        ConnectionNode connectionNode = null;
        try {
            connectionNode = PDKIntegration.createConnectionConnectorBuilder()
                    .withConnectionConfig(DataMap.create(connections.getConfig()))
                    .withGroup(databaseType.getGroup())
                    .withPdkId(databaseType.getPdkId())
                    .withAssociateId(connections.getName() + "_" + ts)
                    .withVersion(databaseType.getVersion())
                    .withLog(new TapLog())
                    .build();
            PDKInvocationMonitor.invoke(connectionNode, PDKMethod.INIT, connectionNode::connectorInit, "Init PDK", TAG);
            JSONObject monitorApi = (JSONObject) connectionNode.getConnectionContext().getSpecification().getConfigOptions().get(MONITOR_API);
            //利用反射去访问连接器的具体方法
            if (monitorApi != null) {
                Update update = new Update();
                ConnectionUpdateOperation.MONITOR_API.set(update, executeMonitorAPIs(connections, connectionNode, monitorApi.clone()));
                clientMongoOperator.update(new Query(Criteria.where("_id").is(connections.getId())), update, ConnectorConstant.CONNECTION_COLLECTION);
            }
        } finally {
            if (connectionNode != null) {
                PDKInvocationMonitor.invoke(connectionNode, PDKMethod.STOP, connectionNode::connectorStop, "Stop PDK", TAG);
                connectionNode.unregisterMemoryFetcher();
            }
            PDKIntegration.releaseAssociateId(connections.getName() + "_" + ts);
        }
    }

    protected Connections parseConnection(Map<String, Object> event) {
        Connections connection;
        try {
            Object schema = ConnectionUpdateOperation.SCHEMA.get(event);
            if (schema instanceof Map) {
                event.remove(ConnectionUpdateOperation.SCHEMA.getFullKey());
            }
            connection = JSONUtil.map2POJO(event, Connections.class);
        } catch (Exception e) {
            throw new ConnectionException("Map convert to Connections failed: " + e.getMessage(), e);
        }

        String pdkHash = ConnectionUpdateOperation.PDK_HASH.getOrDefault(event, "");
        connection.setPdkHash(pdkHash);
        return connection;
    }

    protected DatabaseTypeEnum.DatabaseType getDatabaseType(Connections connection) {
        if (StringUtils.isBlank(connection.getPdkType())) {
            throw new ConnectionException("Unknown connection pdk type");
        }

        DatabaseTypeEnum.DatabaseType databaseDefinition = ConnectionUtil.getDatabaseType(clientMongoOperator, connection.getPdkHash());
        if (databaseDefinition == null) {
            throw new ConnectionException(String.format("Unknown database type %s", connection.getDatabase_type()));
        }
        downloadPdkFile(databaseDefinition);
        return databaseDefinition;
    }

    protected void downloadPdkFile(DatabaseTypeEnum.DatabaseType databaseDefinition) {
        PdkUtil.downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator, databaseDefinition.getPdkHash(), databaseDefinition.getJarFile(), databaseDefinition.getJarRid());
    }

    private Map<String, Object> executeMonitorAPIs(Connections connections, ConnectionNode connectionNode, JSONObject monitorApi) {
        Map<String, Object> result = new ConcurrentHashMap<>();
        CountDownLatch countDownLatch = new CountDownLatch(5);
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        try {
            for (int i = 0; i < 5; i++) {
                executorService.submit(() -> {
                    try {
                        Map.Entry<String, JSONObject> api;
                        while ((api = takeoutMonitorApi(monitorApi)) != null) {
                            try {
                                if (api.getValue().getString("className") != null) {
                                } else if (api.getValue().getString("method") != null) {
                                    String command = api.getValue().getString("method");
                                    Object obj = ReflectionUtil.invokeDeclaredMethod(connectionNode.getConnector(), command.substring(command.lastIndexOf("#") + 1), null);
                                    JSONArray commandsAfter = api.getValue().getJSONArray("methodAfter");
                                    if (CollectionUtils.isNotEmpty(commandsAfter) && obj instanceof Number) {
                                        JSONArray action = null;
                                        for (JSONObject commandAfter : commandsAfter.toArray(new JSONObject[0])) {
                                            Object max = commandAfter.get("max");
                                            Object min = commandAfter.get("min");
                                            if (max instanceof BigDecimal) {
                                                if ((new BigDecimal(String.valueOf(obj))).compareTo((BigDecimal) max) >= 0) {
                                                    continue;
                                                }
                                            }
                                            if (min instanceof BigDecimal) {
                                                if ((new BigDecimal(String.valueOf(obj))).compareTo((BigDecimal) min) < 0) {
                                                    continue;
                                                }
                                            }
                                            action = commandAfter.getJSONArray("action");
                                            break;
                                        }
                                        if (action != null) {
                                            for (String a : action.toArray(new String[0])) {
                                                switch (a) {
                                                    case "sendMail":
                                                        String content = "This Datasource " + api.getKey() + " has been reached " + obj + " it may cause serious issues";
                                                        sendMail(connections, content);
                                                        break;
                                                    case "stopTask":
                                                        stopAllTasks(connections);
                                                        TimeUnit.SECONDS.sleep(10);
                                                        break;
                                                    default:
                                                        if (a.contains("#")) {
                                                            ReflectionUtil.invokeDeclaredMethod(connectionNode.getConnector(), a.substring(a.lastIndexOf("#") + 1), null);
                                                        }
                                                        break;
                                                }
                                            }
                                        }
                                    }
                                    result.put(api.getKey(), obj == null ? "" : String.valueOf(obj));
                                } else if (api.getValue().getString("sqlType") != null) {
                                    ExecuteCommandV2Function executeCommandV2Function = connectionNode.getConnectionFunctions().getExecuteCommandV2Function();
                                    if (executeCommandV2Function == null) {
                                        continue;
                                    }
                                    String key = api.getKey();
                                    String value = api.getValue().getString("sql");
                                    String type = api.getValue().getString("sqlType");
                                    PDKInvocationMonitor.invoke(connectionNode, PDKMethod.EXECUTE_COMMAND, () -> {
                                        executeCommandV2Function.execute(connectionNode.getConnectionContext(), type, value, executeResult -> {
                                            if (CollectionUtils.isNotEmpty(executeResult)) {
                                                DataMap dataMap = executeResult.get(0);
                                                dataMap.entrySet().stream().findFirst().ifPresent(entry -> result.put(key, String.valueOf(entry.getValue())));
                                            }
                                        });
                                    }, TAG);
                                }
                            } catch (Exception e) {
                                logger.warn("Execute monitor api {} failed, {}", api.getKey(), e.getMessage());
                            }
                        }
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } finally {
            executorService.shutdown();
        }
        return result;
    }

    private synchronized Map.Entry<String, JSONObject> takeoutMonitorApi(JSONObject monitorApi) {
        Iterator<Map.Entry<String, Object>> iterator = monitorApi.entrySet().iterator();
        if (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            iterator.remove();
            return Map.entry(next.getKey(), (JSONObject) next.getValue());
        }
        return null;
    }

    private void stopAllTasks(Connections connections) {
        Criteria criteria = new Criteria();
        criteria.and(IS_DELETED).ne(true).and(DAG_NODES_CONNECTION_ID).is(connections.getId()).and(STATUS).is(STATUS_RUNNING);
        Query query = new Query(criteria);
        List<TaskDto> tasks = clientMongoOperator.find(query, "Task", TaskDto.class);
        tasks.forEach(this::stopTask);
    }

    private void stopTask(TaskDto taskDto) {
        String resource = ConnectorConstant.TASK_COLLECTION + "/systemStop";
        try {
            logger.info("Call {} api to modify task [{}] status", resource, taskDto.getName());
            clientMongoOperator.updateById(new Update(), resource, taskDto.getId().toHexString(), TaskOpRespDto.class);
        } catch (Exception e) {
            logger.info("Update task {} failed, {}", resource, e.getMessage());
        }
    }

    private void sendMail(Connections connections, String content) {
        Map<String, Object> param = Maps.newHashMap();
        param.put("connectionName", connections.getName());
        param.put("warningTime", DateUtil.now());
        param.put("warningLog", content);
        AlarmDatasourceDto alarmDto = AlarmDatasourceDto.builder()
                .level(Level.EMERGENCY)
                .agentId(CommonUtils.getenv("process_id"))
                .component(AlarmComponentEnum.FE)
                .status(AlarmStatusEnum.ING)
                .type(AlarmTypeEnum.DATASOURCE_MONITOR_ALARM)
                .connectionId(connections.getId())
                .name(connections.getName())
                .summary(AlarmKeyEnum.DATASOURCE_MONITOR_ALTER.name())
                .metric(AlarmKeyEnum.DATASOURCE_MONITOR_ALTER)
                .param(param)
                .lastNotifyTime(DateUtil.date())
                .build();
        alarmDto.setUserId(connections.getUser_id());
        String resource = ConnectorConstant.TASK_ALARM + "/addDatasourceMsg";
        clientMongoOperator.insertOne(alarmDto, resource);
    }
}
