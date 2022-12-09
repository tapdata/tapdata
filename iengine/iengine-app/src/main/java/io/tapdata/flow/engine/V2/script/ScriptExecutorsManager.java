package io.tapdata.flow.engine.V2.script;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.UUIDGenerator;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableUtil;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Query;
import org.voovan.tools.collection.CacheMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class ScriptExecutorsManager {

  private final Logger logger = LogManager.getLogger(ScriptExecutorsManager.class);

  private final ObsLogger obsLogger;

  private final ClientMongoOperator clientMongoOperator;

  private final HazelcastInstance hazelcastInstance;

  private final CacheMap<String, ScriptExecutor> cacheMap;

  private final String taskId;
  private final String nodeId;


  public ScriptExecutorsManager(ObsLogger obsLogger, ClientMongoOperator clientMongoOperator, HazelcastInstance hazelcastInstance, String taskId, String nodeId) {

    this.taskId = taskId;
    this.nodeId = nodeId;
    this.obsLogger = obsLogger;
    this.clientMongoOperator = clientMongoOperator;
    this.hazelcastInstance = hazelcastInstance;
    this.cacheMap = new CacheMap<String, ScriptExecutor>()
            .supplier(this::create)
            .maxSize(10)
            .autoRemove(true)
            .expire(600)
            .destory((k, v) -> {
              v.close();
              return -1L;
            })
            .create();
  }

  public ScriptExecutor getScriptExecutor(String connectionName) {
    return this.cacheMap.get(connectionName);
  }

  private ScriptExecutor create(String connectionName) {
    Connections connections = clientMongoOperator.findOne(new Query(where("name").is(connectionName)),
            ConnectorConstant.CONNECTION_COLLECTION, Connections.class);

    return new ScriptExecutor(connections, hazelcastInstance, this.getClass().getSimpleName() + "-" + taskId + "-" + nodeId);
  }

  public void close() {
    this.cacheMap.forEach((key, value) -> this.cacheMap.getDestory().apply(key, value));
    this.cacheMap.clear();
  }

  private class ScriptExecutor {

    private final ConnectorNode connectorNode;

    private final String TAG;

    private final String associateId;

    private final TapCodecsFilterManager codecsFilterManager;


    public ScriptExecutor(Connections connections, HazelcastInstance hazelcastInstance, String TAG) {
      this.TAG = TAG;

      Map<String, Object> connectionConfig = connections.getConfig();
      DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connections.getPdkHash());
      //todo 根据connections查询TapTableMap
      TapTableMap<String, TapTable> tapTableMap = TapTableUtil.getTapTableMapByNodeId("");
      PdkTableMap pdkTableMap = new PdkTableMap(tapTableMap);
      PdkStateMap pdkStateMap = new PdkStateMap(TAG, hazelcastInstance, PdkStateMap.StateMapMode.HTTP_TM);
      PdkStateMap globalStateMap = PdkStateMap.globalStateMap(hazelcastInstance);
      this.associateId = this.getClass().getSimpleName() + "-" + connections.getName() + "-" + UUIDGenerator.uuid();
      this.connectorNode = PdkUtil.createNode(TAG,
              databaseType,
              clientMongoOperator,
              associateId,
              connectionConfig,
              pdkTableMap,
              pdkStateMap,
              globalStateMap
      );

      try {
        PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT, connectorNode::connectorInit, TAG);
      } catch (Exception e) {
        throw new RuntimeException("Failed to init pdk connector, database type: " + databaseType + ", message: " + e.getMessage(), e);
      }

      this.codecsFilterManager = connectorNode.getCodecsFilterManager();
    }

    /**
     * executeObj:
     * {
     * //  只对支持sql语句的数据库有效
     * sql: "update order set owner='jk' where order_id=1",
     * <p>
     * // 以下对属性非sql数据库有效
     * op: 'update'      // insert/ update/ delete/ findAndModify
     * database:"inventory",
     * collection:'orders',
     * filter: {name: 'jackin'}  //  条件过滤对象
     * opObject:  {$set:{data_quality: '100'}},    //   操作的数据集
     * upsert: true,     // 是否使用upsert操作， 默认false，只对mongodb的update/ findAndModify有效
     * multi: true        //  是否更新多条记录，默认false
     * }
     *
     * @param executeObj
     * @return
     */
    public long execute(Map<String, Object> executeObj) throws Throwable {

      //generate TapRecordEvent
      List<TapRecordEvent> recordEvents = getTapRecordEventList(executeObj);

      ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
      QueryByFilterFunction queryByFilterFunction = connectorFunctions.getQueryByFilterFunction();
      WriteRecordFunction writeRecordFunction = connectorFunctions.getWriteRecordFunction();

      AtomicReference<WriteListResult<TapRecordEvent>> consumerBack = new AtomicReference<>();
      writeRecordFunction.writeRecord(connectorNode.getConnectorContext(), recordEvents, null,
              consumer -> {
                if (consumer != null) {
                  consumerBack.set(consumer);
                }
              });

      Map<TapRecordEvent, Throwable> errorMap = consumerBack.get().getErrorMap();
      if (MapUtils.isNotEmpty(errorMap)) {
        TapRecordEvent lastErrorTapRecord = null;
        Throwable lastErrorThrowable = null;
        for (Map.Entry<TapRecordEvent, Throwable> tapRecordEventThrowableEntry : errorMap.entrySet()) {
          lastErrorTapRecord = tapRecordEventThrowableEntry.getKey();
          lastErrorThrowable = tapRecordEventThrowableEntry.getValue();
        }
        throw new RuntimeException(String.format("Write record %s failed", lastErrorTapRecord), lastErrorThrowable);
      }
      return consumerBack.get().getModifiedCount();
    }

    private List<TapRecordEvent> getTapRecordEventList(Map<String, Object> executeObj) {
      return null;
    }

    public List<Map<String, Object>> executeQuery(Map<String, Object> executeObj) {
      return null;
    }

    void close() {

      CommonUtils.handleAnyError(() -> {
        Optional.ofNullable(connectorNode)
                .ifPresent(connectorNode -> {
                  PDKInvocationMonitor.stop(connectorNode);
                  PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, TAG);
                });
        logger.info("PDK connector node stopped: " + associateId);
        obsLogger.info("PDK connector node stopped: " + associateId);
      }, err -> {
        logger.warn(String.format("Stop PDK connector node failed: %s | Associate id: %s", err.getMessage(), associateId));
        obsLogger.warn(String.format("Stop PDK connector node failed: %s | Associate id: %s", err.getMessage(), associateId));
      });
    }

  }
}
