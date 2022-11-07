package com.tapdata.cache;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.UUIDGenerator;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class PdkDataSourceRowsGetter implements IDataSourceRowsGetter {

  private final ConnectorNode connectorNode;
  private final String TAG;

  private final TapTable tapTable;

  private final DataFlowCacheConfig cacheConfig;
  private final String associateId;

  private final TapCodecsFilterManager codecsFilterManager;

  public PdkDataSourceRowsGetter(DataFlowCacheConfig dataFlowCacheConfig,
                                 ClientMongoOperator clientMongoOperator,
                                 HazelcastInstance hazelcastInstance) {
    this.cacheConfig = dataFlowCacheConfig;

    Node sourceNode = dataFlowCacheConfig.getSourceNode();
    Connections sourceConnection = dataFlowCacheConfig.getSourceConnection();
    Map<String, Object> connectionConfig = sourceConnection.getConfig();
    DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, sourceConnection.getPdkHash());
    TapTableMap<String, TapTable> tapTableMap = TapTableUtil.getTapTableMapByNodeId(sourceNode.getId());
    PdkTableMap pdkTableMap = new PdkTableMap(tapTableMap);
    PdkStateMap pdkStateMap = new PdkStateMap(sourceNode.getId(), hazelcastInstance, PdkStateMap.StateMapMode.HTTP_TM);
    PdkStateMap globalStateMap = PdkStateMap.globalStateMap(hazelcastInstance);
    this.tapTable = tapTableMap.get(dataFlowCacheConfig.getTableName());
    this.associateId = this.getClass().getSimpleName() + "-" + sourceNode.getId() + "-" + UUIDGenerator.uuid();
    this.connectorNode = PdkUtil.createNode(dataFlowCacheConfig.getCacheName(),
            databaseType,
            clientMongoOperator,
            associateId,
            connectionConfig,
            pdkTableMap,
            pdkStateMap,
            globalStateMap
    );

    try {
      TAG = this.getClass().getSimpleName();
      PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT, connectorNode::connectorInit, TAG);
    } catch (Exception e) {
      throw new RuntimeException("Failed to init pdk connector, database type: " + databaseType + ", message: " + e.getMessage(), e);
    }

    this.codecsFilterManager = connectorNode.getCodecsFilterManager();
  }

  @Override
  public List<Map<String, Object>> getRows(Object[] values) {
    AtomicReference<List<Map<String, Object>>> resultsAtomic = new AtomicReference<>();
    QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = connectorNode.getConnectorFunctions().getQueryByAdvanceFilterFunction();
    TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create();

    if (values == null) {
      values = new Object[0];
    }
    String cacheKeys = this.cacheConfig.getCacheKeys();
    String[] fields = cacheKeys.split(",");
    DataMap dataMap = new DataMap();
    for (int i = 0; i < fields.length; i++) {
      if (i < values.length) {
        dataMap.put(fields[i], values[i]);
      } else {
        dataMap.put(fields[i], null);
      }
    }

    tapAdvanceFilter.match(dataMap);
    PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
            () -> queryByAdvanceFilterFunction.query(connectorNode.getConnectorContext(), tapAdvanceFilter, tapTable,
                    filterResults -> {
                      List<Map<String, Object>> results = filterResults.getResults();
                      if (CollectionUtils.isNotEmpty(results)) {
                        resultsAtomic.set(new ArrayList<>(results));
                      }
                    }),
            TAG);
    List<Map<String, Object>> maps = resultsAtomic.get();
    if (CollectionUtils.isNotEmpty(maps)) {
      for (Map<String, Object> map : maps) {
        codecsFilterManager.transformToTapValueMap(map, tapTable.getNameFieldMap());
        codecsFilterManager.transformFromTapValueMap(map);
      }
    }


    return maps;
  }

  @Override
  public void close() {
    if (null != connectorNode) {
      PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, TAG);
      PDKIntegration.releaseAssociateId(this.associateId);
    }
  }
}
