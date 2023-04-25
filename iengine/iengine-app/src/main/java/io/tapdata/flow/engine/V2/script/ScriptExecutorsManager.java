package io.tapdata.flow.engine.V2.script;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.UUIDGenerator;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.log.LogFactory;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableMap;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Query;
import org.voovan.tools.collection.CacheMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class ScriptExecutorsManager {

	private final Log scriptLogger;

	private final ClientMongoOperator clientMongoOperator;

	private final HazelcastInstance hazelcastInstance;

	private final CacheMap<String, ScriptExecutor> cacheMap;

	private final String taskId;
	private final String nodeId;

	private final boolean trialRun;

	public ScriptExecutorsManager(Log scriptLogger, ClientMongoOperator clientMongoOperator, HazelcastInstance hazelcastInstance, String taskId, String nodeId) {
		this(scriptLogger, clientMongoOperator, hazelcastInstance, taskId, nodeId, false);
	}

	public ScriptExecutorsManager(Log scriptLogger, ClientMongoOperator clientMongoOperator, HazelcastInstance hazelcastInstance, String taskId, String nodeId, boolean trialRun) {

		this.taskId = taskId;
		this.nodeId = nodeId;
		this.scriptLogger = scriptLogger;
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
		this.trialRun = trialRun;
	}

	public ScriptExecutor getScriptExecutor(String connectionName) {
		ScriptExecutor scriptExecutor = this.cacheMap.get(connectionName);
		if (scriptExecutor == null) {
			throw new IllegalArgumentException("The specified connection source [" + connectionName + "] could not build the executor, please check");
		}
		return scriptExecutor;
	}

	private ScriptExecutor create(String connectionName) {
		Connections connections = clientMongoOperator.findOne(new Query(where("name").is(connectionName)),
				ConnectorConstant.CONNECTION_COLLECTION, Connections.class);

		if (connections == null) {
			throw new IllegalArgumentException("The specified connection source [" + connectionName + "] does not exist, please check");
		}

		scriptLogger.info("create script executor for {}", connectionName);

		return create(connections, clientMongoOperator, hazelcastInstance, scriptLogger);
	}

	public ScriptExecutor create(Connections connections, ClientMongoOperator clientMongoOperator, HazelcastInstance hazelcastInstance, Log scriptLogger) {
		return new ScriptExecutor(connections, clientMongoOperator, hazelcastInstance, scriptLogger,
				this.getClass().getSimpleName() + "-" + taskId + "-" + nodeId + "-" + UUIDGenerator.uuid(), trialRun);
	}

	public void close() {
		this.cacheMap.forEach((key, value) -> this.cacheMap.getDestory().apply(key, value));
		this.cacheMap.clear();
	}

	public static class ScriptExecutor {

		private final ConnectorNode connectorNode;

		private final String TAG;

		private final String associateId;
		private final Supplier<ExecuteCommandFunction> executeCommandFunctionSupplier;

		private final TapTableMap<String, TapTable> tapTableMap;

		private final Log scriptLogger;

		private final boolean trialRun;

		private ScriptExecutor(Connections connections, ClientMongoOperator clientMongoOperator, HazelcastInstance hazelcastInstance, Log scriptLogger, String TAG, boolean trialRun) {
			this.TAG = TAG;
			this.scriptLogger = scriptLogger;
			this.trialRun = trialRun;

			Map<String, Object> connectionConfig = connections.getConfig();
			DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connections.getPdkHash());
			PdkStateMap pdkStateMap = new PdkStateMap(TAG, hazelcastInstance);
			PdkStateMap globalStateMap = PdkStateMap.globalStateMap(hazelcastInstance);
			this.tapTableMap = TapTableMap.create("ScriptExecutor", TAG);
			PdkTableMap pdkTableMap = new PdkTableMap(tapTableMap);
			this.associateId = this.getClass().getSimpleName() + "-" + connections.getName() + "-" + UUIDGenerator.uuid();
			this.connectorNode = PdkUtil.createNode(TAG,
					databaseType,
					clientMongoOperator,
					associateId,
					connectionConfig,
					pdkTableMap,
					pdkStateMap,
					globalStateMap,
					InstanceFactory.instance(LogFactory.class).getLog()
			);

			try {
				PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT, connectorNode::connectorInit, TAG);
			} catch (Exception e) {
				throw new RuntimeException("Failed to init pdk connector, database type: " + databaseType + ", message: " + e.getMessage(), e);
			}

			ConnectorFunctions connectorFunctions = this.connectorNode.getConnectorFunctions();
			this.executeCommandFunctionSupplier = connectorFunctions::getExecuteCommandFunction;

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
			if (trialRun) {
				scriptLogger.info("Trial run, skip execute");
				return 0;
			}
			ExecuteResult<Long> executeResult = new ExecuteResult<>();
			pdkExecute("execute", executeObj, executeResult);
			return executeResult.getResult();
		}

		public List<? extends Map<String, Object>> executeQuery(Map<String, Object> executeObj) throws Throwable {
			ExecuteResult<List<Map<String, Object>>> executeResult = new ExecuteResult<>();
			pdkExecute("executeQuery", executeObj, executeResult);
			return executeResult.getResult();
		}

		public long count(Map<String, Object> executeObj) throws Throwable {
			ExecuteResult<Long> executeResult = new ExecuteResult<>();
			pdkExecute("count", executeObj, executeResult);
			return executeResult.getResult();
		}

		public List<? extends Map<String, Object>> aggregate(Map<String, Object> executeObj) throws Throwable {
			ExecuteResult<List<? extends Map<String, Object>>> executeResult = new ExecuteResult<>();
			pdkExecute("aggregate", executeObj, executeResult);
			return executeResult.getResult();
		}

		public Object call(String funcName, List<Map<String, Object>> params) throws Throwable {
			if (trialRun) {
				scriptLogger.info("Trial run, skip call");
				return new Object();
			}
			ExecuteResult<Long> executeResult = new ExecuteResult<>();
			Map<String, Object> executeObj = new HashMap<>();
			executeObj.put("funcName", funcName);
			executeObj.put("params", params);
			pdkExecute("call", executeObj, executeResult);
			return executeResult.getResult();
		}

		private <T> void pdkExecute(String command, Map<String, Object> executeObj, ExecuteResult<T> executeResult) throws Throwable {
			ExecuteCommandFunction executeCommandFunction = this.executeCommandFunctionSupplier.get();
			if (executeCommandFunction == null) {
				TapNodeSpecification specification = this.connectorNode.getConnectorContext().getSpecification();
				String tag = specification.getName() + "-" + specification.getVersion();
				throw new RuntimeException("pdk [" + tag + "] not support execute command");
			}
			TapExecuteCommand executeCommand = getTapExecuteCommand(command, executeObj);
			executeCommandFunction.execute(this.connectorNode.getConnectorContext(), executeCommand, e -> {

				if (e.getError() != null) {
					executeResult.setError(e.getError());
					return;
				}
				if (executeResult.getResult() != null && executeResult.getResult() instanceof List) {
					((List) executeResult.getResult()).addAll((Collection) e.getResult());
				} else {
					executeResult.setResult((T) e.getResult());
				}
			});

			if (executeResult == null || executeResult.getError() != null) {
				throw new RuntimeException("script execute error", executeResult == null ? new NullPointerException() : executeResult.getError());
			}
		}

		@NotNull
		private TapExecuteCommand getTapExecuteCommand(String command, Map<String, Object> executeObj) {
			return TapExecuteCommand.create().command(command).params(executeObj);
		}


		public void close() {

			CommonUtils.handleAnyError(() -> {
				Optional.ofNullable(connectorNode)
						.ifPresent(connectorNode -> {
							PDKInvocationMonitor.stop(connectorNode);
							PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, TAG);
						});
				scriptLogger.info("PDK connector node stopped: " + associateId);
			}, err -> scriptLogger.warn(String.format("Stop PDK connector node failed: %s | Associate id: %s", err.getMessage(), associateId)));
			CommonUtils.handleAnyError(() -> {
				Optional.ofNullable(connectorNode).ifPresent(node -> PDKIntegration.releaseAssociateId(associateId));
				scriptLogger.info("PDK connector node released: " + associateId);
			}, err -> scriptLogger.warn(String.format("Release PDK connector node failed: %s | Associate id: %s", err.getMessage(), associateId)));
			CommonUtils.handleAnyError(() -> {
				Optional.ofNullable(tapTableMap).ifPresent(TapTableMap::reset);
				scriptLogger.info(String.format("[%s] schema data cleaned", TAG));
			}, err -> scriptLogger.warn(String.format("Clean [%s] schema data failed: %s", TAG, err.getMessage())));
		}

	}
}
