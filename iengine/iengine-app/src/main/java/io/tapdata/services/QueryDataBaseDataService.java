package io.tapdata.services;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.entity.EmptyMap;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.GetTableInfoFunction;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.EmptyTapTableMap;
import io.tapdata.schema.TapTableUtil;
import io.tapdata.service.skeleton.annotation.RemoteService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

@RemoteService
@Slf4j
public class QueryDataBaseDataService {

	public static final int rows = 100;

	private TapCodecsFilterManager originCodecsFilterManager;

	public QueryDataBaseDataService() {
		TapCodecsRegistry codecsRegistry = TapCodecsRegistry.create();
		codecsRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> {
			return formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss");
		});
		originCodecsFilterManager = TapCodecsFilterManager.create(codecsRegistry);
	}


	public static String formatTapDateTime(DateTime dateTime, String pattern) {
		try {
			DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern);
			final ZoneId zoneId = dateTime.getTimeZone() != null ? dateTime.getTimeZone().toZoneId() : ZoneId.of("GMT");
			LocalDateTime localDateTime = LocalDateTime.ofInstant(dateTime.toInstant(), zoneId);
			return dateTimeFormatter.format(localDateTime);
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}

	public Map<String, Object> getData(String connectionId, String tableName) throws Throwable {
		String associateId = "queryRecords_" + connectionId + "_" + tableName + "_" + UUID.randomUUID();
		try {
			ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
			Connections connections = HazelcastTaskService.taskService().getConnection(connectionId);
			DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connections.getPdkHash());
			TapTable tapTable = TapTableUtil.getTapTableByConnectionId(connectionId, tableName);
			if (tapTable == null) {
				log.error("Cannot find tableName :{}", tableName);
				return new HashMap<>();
				//throw new RuntimeException("Cannot find tableName:" + tableName);
			}
			ConnectorNode connectorNode = createConnectorNode(associateId, (HttpClientMongoOperator) clientMongoOperator, databaseType, connections.getConfig());
			List<Map<String, Object>> maps;
			String TAG = this.getClass().getSimpleName();
			try {
				PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT, connectorNode::connectorInit, TAG);
				//queryByAdvanceFilter
				TapCodecsFilterManager codecsFilterManager = connectorNode.getCodecsFilterManager();
				AtomicReference<List<Map<String, Object>>> resultsAtomic = new AtomicReference<>();
				QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = connectorNode.getConnectorFunctions().getQueryByAdvanceFilterFunction();
				TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create();
				tapAdvanceFilter.limit(rows);
				try {
					queryByAdvanceFilterFunction.query(connectorNode.getConnectorContext(), tapAdvanceFilter, tapTable,
							filterResults -> {
								List<Map<String, Object>> results = filterResults.getResults();
								if (CollectionUtils.isNotEmpty(results)) {
									resultsAtomic.set(new ArrayList<>(results));
								}
							});
					maps = resultsAtomic.get();
					if (CollectionUtils.isNotEmpty(maps)) {
						for (Map<String, Object> map : maps) {
							codecsFilterManager.transformToTapValueMap(map, tapTable.getNameFieldMap());
							originCodecsFilterManager.transformFromTapValueMap(map);
						}
					}
				} catch (Exception e1) {
					log.error("Query ByAdvanceFilterFunction error :", e1);
					maps = resultsAtomic.get();
				}
				TableInfo tableInfo = TableInfo.create();
				try {
					GetTableInfoFunction getTableInfoFunction = connectorNode.getConnectorFunctions().getGetTableInfoFunction();
					if (getTableInfoFunction == null) {
						tableInfo.setNumOfRows(0L);
						tableInfo.setStorageSize(0L); // 字节单位
					} else {
						tableInfo = getTableInfoFunction.getTableInfo(connectorNode.getConnectorContext(), tableName);
					}
				} catch (Exception e) {
					log.error("Get TableInfoFunction error :", e);
					tableInfo.setNumOfRows(0L);
					tableInfo.setStorageSize(0L); // 字节单位
				}
				return map(entry("sampleData", maps), entry("tableInfo", tableInfo));
			} catch (Exception e) {
				log.error("Failed to init pdk connector, database type: " + databaseType + ", message: " + e.getMessage(), e);
				return map(entry("sampleData", new ArrayList<>()), entry("tableInfo", new TableInfo()));
			} finally {
				try {
					PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, TAG);
				} catch (Exception e) {
					log.error(" Stop error{}", e.getMessage());
				}
			}
		} finally {
			PDKIntegration.releaseAssociateId(associateId);
		}

	}


	private ConnectorNode createConnectorNode(String associateId, HttpClientMongoOperator clientMongoOperator, DatabaseTypeEnum.DatabaseType databaseType, Map<String, Object> connectionConfig) {
		try {
			PdkUtil.downloadPdkFileIfNeed(clientMongoOperator,
					databaseType.getPdkHash(), databaseType.getJarFile(), databaseType.getJarRid());
			PDKIntegration.ConnectorBuilder<ConnectorNode> connectorBuilder = PDKIntegration.createConnectorBuilder()
					.withDagId(associateId)
					.withAssociateId(associateId)
					.withConfigContext(null)
					.withGroup(databaseType.getGroup())
					.withVersion(databaseType.getVersion())
					.withPdkId(databaseType.getPdkId())
					.withTableMap(new EmptyTapTableMap())
					.withStateMap(new EmptyMap())
					.withGlobalStateMap(new EmptyMap());
			if (MapUtils.isNotEmpty(connectionConfig)) {
				connectorBuilder.withConnectionConfig(DataMap.create(connectionConfig));
			}
			return connectorBuilder.build();
		} catch (Exception e) {
			throw new RuntimeException("Failed to create pdk connector node, database type: " + databaseType + ", message: " + e.getMessage(), e);
		}
	}


//    public TableInfo getTableInfo(String connectionId, String tableName) throws Throwable {
//        String associateId = "queryTableInfo_" + connectionId + "_" + tableName + "_" + UUID.randomUUID();
//        try {
//            ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
//            Connections connections = HazelcastTaskService.taskService().getConnection(connectionId);
//            DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connections.getPdkHash());
//            ConnectorNode connectorNode = createConnectorNode(associateId, (HttpClientMongoOperator) clientMongoOperator, databaseType, connections.getConfig());
//            String TAG = this.getClass().getSimpleName();
//            try {
//                PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT, connectorNode::connectorInit, TAG);
//                //queryByAdvanceFilter
//                TableInfo tableInfo = TableInfo.create();
//                GetTableInfoFunction getTableInfoFunction = connectorNode.getConnectorFunctions().getGetTableInfoFunction();
//                if (getTableInfoFunction == null) {
//                    tableInfo.setNumOfRows(0L);
//                    tableInfo.setStorageSize(0L); // 字节单位
//                    return tableInfo;
//                }
//                tableInfo = getTableInfoFunction.getTableInfo(connectorNode.getConnectorContext(), tableName);
//                return tableInfo;
//            } catch (Exception e) {
//                throw new RuntimeException("Failed to init pdk connector, database type: " + databaseType + ", message: " + e.getMessage(), e);
//            } finally {
//                PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, TAG);
//            }
//        } finally {
//            PDKIntegration.releaseAssociateId(associateId);
//        }
//
//    }

}
