package io.tapdata.services;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.CollectLog;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.FormatUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.exception.ConnectionException;
import io.tapdata.modules.api.pdk.PDKUtils;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.TapTableUtil;
import io.tapdata.service.skeleton.annotation.RemoteService;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RemoteService
public class TestRunService {

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

    /**
     * connections
     */
    private Connections connections;

    private Map<String, Object> param;

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


  public Response testRun(Request request) throws Throwable {

    CollectLog collectLog = getCollectLog();
    String version = request == null ? "" : request.getVersion();
    Response response = new Response(collectLog.getLogs(), version);
    if (request == null) {
      collectLog.error("request param is null");
      return response;
    }

    Connections connections = request.getConnections();
    if (connections == null) {
      collectLog.error("connections is null");
      return response;
    }
    String pdkType = connections.getPdkType();
    if (StringUtils.isBlank(pdkType)) {
      throw new ConnectionException("Unknown connection pdk type");
    }


    ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
    DatabaseTypeEnum.DatabaseType databaseDefinition = ConnectionUtil.getDatabaseType(clientMongoOperator, pdkType);
    if (databaseDefinition == null) {
      throw new ConnectionException(String.format("Unknown database type %s", connections.getDatabase_type()));
    }

    PDKUtils pdkUtils = InstanceFactory.instance(PDKUtils.class);
    pdkUtils.downloadPdkFileIfNeed(connections.getPdkHash());

    long ts = System.currentTimeMillis();
    String associateId = "TEST_RUN-" + connections.getName() + "_" + ts;
    ConnectorNode connectorNode = null;
    try {

      String tableName = (String) connections.getConfig().get("");
      TapTable tapTable = TapTableUtil.getTapTableByConnectionId(connections.getId(), tableName);
      connectorNode = PDKIntegration.createConnectorBuilder()
              .withDagId("")
              .withGroup(databaseDefinition.getGroup())
              .withPdkId(databaseDefinition.getPdkId())
              .withVersion(databaseDefinition.getVersion())
              .withAssociateId(associateId)
              .withConnectionConfig(DataMap.create(connections.getConfig()))
              .withLog(collectLog)
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
            BatchReadFunction batchReadFunction = connectorFunctions.getBatchReadFunction();
            PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_BATCH_READ,
                    () -> batchReadFunction.batchRead(connectorContext, tapTable, null, 512,
                            (events, offsetObject) -> collectLog.info("initial_sync: {} {}", events, offsetObject)),
                    TAG);
          } else if (StringUtils.contains(syncType, "cdc")) {
            // cdc
            StreamReadFunction streamReadFunction = connectorFunctions.getStreamReadFunction();
            PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_STREAM_READ,
                    () -> streamReadFunction.streamRead(connectorContext, Collections.singletonList("tableName"), null, 512,
                            StreamReadConsumer.create((events, offsetObject) -> collectLog.info("cdc: {} {}", events, offsetObject))),
                    TAG);
          } else {
            collectLog.error("Unsupported sync type: {}", syncType);
          }

        } else if (StringUtils.equals(type, "target")) {
          // target
          List<TapRecordEvent> recordEvents = new ArrayList<>();
          WriteRecordFunction writeRecordFunction = connectorFunctions.getWriteRecordFunction();
          PDKInvocationMonitor.invoke(connectorNode, PDKMethod.TARGET_WRITE_RECORD,
                  () -> writeRecordFunction.writeRecord(connectorContext, recordEvents, tapTable, writeListResult -> {
                    Map<TapRecordEvent, Throwable> errorMap = writeListResult.getErrorMap();
                    if (MapUtils.isNotEmpty(errorMap)) {
                      for (Map.Entry<TapRecordEvent, Throwable> entry : errorMap.entrySet()) {
                        collectLog.error("Error record {} {}", entry.getKey(), entry.getValue());
                      }
                    }
                    response.incrementInserted(writeListResult.getInsertedCount());
                    response.incrementModified(writeListResult.getModifiedCount());
                    response.incrementRemove(writeListResult.getRemovedCount());
                  }),
                  TAG);
        } else {
          collectLog.error("Unsupported type: {}", type);
        }


      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, "Stop PDK", TAG);
      }
    } finally {
      if(connectorNode != null) {
        connectorNode.unregisterMemoryFetcher();
      }
      PDKIntegration.releaseAssociateId(associateId);
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
