package io.tapdata.inspect;

import com.tapdata.constant.CollectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.inspect.Inspect;
import com.tapdata.entity.inspect.InspectDataSource;
import com.tapdata.entity.inspect.InspectDetail;
import com.tapdata.entity.inspect.InspectMethod;
import com.tapdata.entity.inspect.InspectResult;
import com.tapdata.entity.inspect.InspectStatus;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.SettingService;
import io.tapdata.inspect.cdc.InspectCdcUtils;
import io.tapdata.inspect.cdc.compare.RowCountInspectCdcJob;
import io.tapdata.inspect.compare.TableRowContentInspectJob;
import io.tapdata.inspect.compare.TableRowCountInspectJob;
import io.tapdata.inspect.compare.TableRowScriptInspectJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/8/20 3:21 下午
 * @description
 */
public class InspectService {

	private static final ThreadPoolExecutor executorService = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
			60L, TimeUnit.SECONDS, new SynchronousQueue<>());
	private ClientMongoOperator clientMongoOperator;
	private Logger logger = LogManager.getLogger(InspectService.class);
	private ConcurrentHashMap RUNNING_INSPECT = new ConcurrentHashMap<>();
	private SettingService settingService;

	private InspectService() {
	}

	public static InspectService getInstance(ClientMongoOperator clientMongoOperator, SettingService settingService) {
		return InspectServiceInstance.INSTANCE.getInstance()
				.setClientMongoOperator(clientMongoOperator)
				.setSettingService(settingService);
	}

	private InspectService setSettingService(SettingService settingService) {
		if (this.settingService == null) {
			synchronized (this) {
				if (this.settingService == null) {
					this.settingService = settingService;
				}
			}
		}
		return this;
	}

	private InspectService setClientMongoOperator(ClientMongoOperator clientMongoOperator) {
		if (this.clientMongoOperator == null) {
			synchronized (this) {
				if (this.clientMongoOperator == null) {
					this.clientMongoOperator = clientMongoOperator;
				}
			}
		}
		return this;
	}

	/**
	 * InspectService singleton
	 */
	private enum InspectServiceInstance {
		INSTANCE();

		private final InspectService inspectService;

		InspectServiceInstance() {
			inspectService = new InspectService();
		}

		public InspectService getInstance() {
			return inspectService;
		}
	}

	/**
	 * Query the data verification task that needs to be performed
	 *
	 * @return
	 */
	public Inspect getInspectById(String id) {
		Query query = Query.query(Criteria.where("_id").is(id));
		query.fields().exclude("tasks.source.fields").exclude("tasks.target.fields");
		return clientMongoOperator.findOne(query, ConnectorConstant.INSPECT_COLLECTION, Inspect.class);
	}

	public void updateStatus(String id, InspectStatus status, String msg) {

		Map<String, Object> queryMap = new HashMap<>();
		queryMap.put("id", id);

		Map<String, Object> updateMap = new HashMap<>();
		updateMap.put("status", status.getCode());
		updateMap.put("errorMsg", msg);

		clientMongoOperator.upsert(queryMap, updateMap, ConnectorConstant.INSPECT_COLLECTION);
	}

	/**
	 * 更新或插入检查结果
	 * 默认过滤inspect属性
	 *
	 * @param inspectResult
	 */
	public void upsertInspectResult(InspectResult inspectResult, boolean excludeInspect) {

		if (inspectResult.getId() != null) {
			Query query = Query.query(Criteria.where("_id").is(inspectResult.getId()));

			try {
				Map<String, Object> update = MapUtil.obj2Map(inspectResult);
				update.remove("id");
				if (excludeInspect) {
					update.remove("inspect");
				}
				clientMongoOperator.upsert(query.getQueryObject(), update, ConnectorConstant.INSPECT_RESULT_COLLECTION);
			} catch (IllegalAccessException e) {
				logger.error("Update inspect result failed. ", e);
			}
		} else {
			clientMongoOperator.insertOne(inspectResult, ConnectorConstant.INSPECT_RESULT_COLLECTION);
		}
	}

	public void upsertInspectResult(InspectResult inspectResult) {
		upsertInspectResult(inspectResult, true);
	}

	/**
	 * 获取最后一次差异结果
	 *
	 * @param firstCheckId 初次校验编号
	 * @return 差异结果
	 */
	public InspectResult getLastDifferenceInspectResult(String firstCheckId) {
		Query query = Query.query(Criteria
				.where("firstCheckId").regex("^" + firstCheckId + "$")
				.and("status").is(InspectStatus.DONE.getCode())
				.and("stats.status").is(InspectStatus.DONE.getCode())
				.and("stats.result").is("failed")
		).with(Sort.by(Sort.Order.desc("ttlTime"))).limit(1);
		return clientMongoOperator.findOne(query, ConnectorConstant.INSPECT_RESULT_COLLECTION, InspectResult.class);
	}

	/**
	 * 获取最后一次校验结果
	 *
	 * @param inspectId 校验任务编号
	 * @return 校验结果
	 */
	public InspectResult getLastInspectResult(String inspectId) {
		Query query = Query.query(Criteria
				.where("inspect_id").regex("^" + inspectId + "$")
				.and("status").in(InspectStatus.DONE.getCode(), InspectStatus.PAUSE.getCode(), InspectStatus.ERROR.getCode())
		).with(Sort.by(Sort.Order.desc("ttlTime"))).limit(1);
		return clientMongoOperator.findOne(query, ConnectorConstant.INSPECT_RESULT_COLLECTION, InspectResult.class);
	}

	/**
	 * 根据 id 查询 inspect result
	 *
	 * @param inspectResultId 校验结果id
	 * @return 校验结果
	 */
	public InspectResult getInspectResultById(String inspectResultId) {
		return clientMongoOperator.findOne(Query.query(Criteria.where("id").is(inspectResultId)),
				ConnectorConstant.INSPECT_RESULT_COLLECTION, InspectResult.class);
	}

	public List<Connections> getInspectConnectionsById(Inspect inspect) {
		/*String idArray = "\"" + String.join("\",\"", ids) + "\"";
		String query = "{\"id\": {\"inq\": " + idArray + "}}";*/

		Set<String> connectionIds = new HashSet<>();
		inspect.getTasks().forEach(task -> {
			connectionIds.add(task.getSource().getConnectionId());
			connectionIds.add(task.getTarget().getConnectionId());
		});

		Query query = Query.query(Criteria.where("id").in(connectionIds));
		query.fields().exclude("response_body").exclude("schema");
		List<Connections> connections = MongodbUtil.getConnections(query, null, clientMongoOperator, true);
		return connections;
	}

	public void insertInspectDetails(List<InspectDetail> details) {
		if (!CollectionUtil.isEmpty(details)) {
			clientMongoOperator.insertList(details, ConnectorConstant.INSPECT_DETAILS_COLLECTION);
		}
	}

	/**
	 * Start up data verification task
	 *
	 * @param inspect
	 */
	public void startInspect(Inspect inspect) {
		if (inspect == null) {
			throw new IllegalArgumentException("inspect can not be empty.");
		}
		logger.info(String.format("Start up data verification %s(%s, %s) ", inspect.getName(), inspect.getId(), inspect.getInspectMethod()));

		if (RUNNING_INSPECT.containsKey(inspect.getId())) {
			logger.warn("Data verification is running {}({}, {}) ", inspect.getName(), inspect.getId(), inspect.getInspectMethod());
			return;
		}
		RUNNING_INSPECT.put(inspect.getId(), inspect);

		InspectMethod inspectMethod = InspectMethod.get(inspect.getInspectMethod());
		switch (inspectMethod) {
			case FIELD:
			case JOINTFIELD:
				executeFieldInspect(inspect);
				break;
			case CDC_COUNT:
			case ROW_COUNT:
				executeRowCountInspect(inspect);
				break;
			default:
				logger.error("Unsupported comparison method '{}', inspect id '{}': `{}'", inspectMethod, inspect.getId(), inspect.getName());
				RUNNING_INSPECT.remove(inspect.getId());
				updateStatus(inspect.getId(), InspectStatus.ERROR, String.join(", ", "Unsupported comparison method"));
				break;
		}
	}

	public void stopInspect(Inspect inspect) {
		RUNNING_INSPECT.remove(inspect.getId());
	}

	/**
	 * 执行行数检查
	 *
	 * @param inspect
	 */
	private void executeRowCountInspect(Inspect inspect) {
		List<String> errorMsg = checkRowCountInspect(inspect);
		if (errorMsg.size() > 0) {
			updateStatus(inspect.getId(), InspectStatus.ERROR, String.join(", ", errorMsg));
			return;
		}

		submitTask(new io.tapdata.inspect.InspectTask(this, inspect, clientMongoOperator) {

			@Override
			public Runnable createTableInspectJob(InspectTaskContext inspectTaskContext) {
				if (InspectCdcUtils.isInspectCdc(inspect)) {
					InspectCdcUtils.initCdcRunProfiles(inspect, inspectTaskContext.getTask());
					return new RowCountInspectCdcJob(inspectTaskContext);
				}
				return new TableRowCountInspectJob(inspectTaskContext);
			}
		});
	}

	/**
	 * 执行字段检查
	 *
	 * @param inspect
	 */
	private void executeFieldInspect(Inspect inspect) {
		List<String> errorMsg = checkFieldInspect(inspect);
		if (errorMsg.size() > 0) {
			updateStatus(inspect.getId(), InspectStatus.ERROR, String.join(", ", errorMsg));
			return;
		}

		submitTask(new io.tapdata.inspect.InspectTask(this, inspect, clientMongoOperator) {

			@Override
			public Runnable createTableInspectJob(InspectTaskContext inspectTaskContext) {
				// 高级校验，进入 TableRowScriptInspectJob
				// 非全匹配，不走高级校验
				if (!inspectTaskContext.getTask().isFullMatch() || !inspectTaskContext.getTask().isShowAdvancedVerification()) {
					return new TableRowContentInspectJob(inspectTaskContext);
				} else {
					return new TableRowScriptInspectJob(inspectTaskContext);
				}
			}
		});
	}

	private Future submitTask(Runnable task) {
		return executorService.submit(task);
	}

	/**
	 * 检查行数比对参数
	 *
	 * @param inspect
	 * @return
	 */
	private List<String> checkRowCountInspect(Inspect inspect) {
		/*
		 * 1. Status must be scheduling
		 * 2. tasks size must gt 0
		 * 3. tasks.source、tasks.target、tasks.taskId can not be empty.
		 * 4. tasks.source.connectionId、tasks.source.table can not be empty.
		 */
		List<String> errorMsg = new ArrayList<>();

		if (inspect == null) {
			errorMsg.add("Inspect can not be empty.");
			return errorMsg;
		}

		if (!"scheduling".equals(inspect.getStatus())) {
			errorMsg.add("Inspect status must be scheduling");
		}

		if (inspect.getTasks() == null || inspect.getTasks().size() == 0) {
			errorMsg.add("Inspect sub-task can not be empty.");
		}

		for (int i = 0; i < inspect.getTasks().size(); i++) {
			com.tapdata.entity.inspect.InspectTask task = inspect.getTasks().get(i);
			if (task == null) {
				logger.warn("Inspect.tasks[" + i + "] is empty.");
				continue;
			}
			if (StringUtils.isEmpty(task.getTaskId())) {
				errorMsg.add("Inspect.tasks[" + i + "].taskId can not be empty");
			}
			checkRowCountInspectTaskDataSource("Inspect.tasks[" + i + "].source", task.getSource());
			checkRowCountInspectTaskDataSource("Inspect.tasks[" + i + "].target", task.getTarget());
		}
		return errorMsg;
	}

	private List<String> checkRowCountInspectTaskDataSource(String prefix, InspectDataSource dataSource) {
		List<String> errorMsg = new ArrayList<>();
		if (StringUtils.isEmpty(dataSource.getConnectionId())) {
			errorMsg.add(prefix + ".connectionId can not be empty.");
		}
		if (StringUtils.isEmpty(dataSource.getTable())) {
			errorMsg.add(prefix + ".table can not be empty.");
		}
		return errorMsg;
	}

	/**
	 * 检查字段比对参数
	 *
	 * @param inspect
	 */
	private List<String> checkFieldInspect(Inspect inspect) {

		/*
		 * 1. Status must be scheduling
		 * 2. limit can not be empty.
		 * 3. tasks size must gt 0
		 * 4. tasks.source、tasks.target、tasks.taskId can not be empty.
		 * 5. tasks.source.connectionId、tasks.source.table、tasks.source.sortColumn、
		 * 	  tasks.source.direction can not be empty.
		 */

		List<String> errorMsg = new ArrayList<>();

		if (inspect == null) {
			errorMsg.add("Inspect can not be empty.");
			return errorMsg;
		}

		if (!"scheduling".equals(inspect.getStatus())) {
			errorMsg.add("Inspect status must be scheduling");
		}

		if (inspect.getTasks() == null || inspect.getTasks().size() == 0) {
			errorMsg.add("Inspect sub-task can not be empty.");
		}

		if (inspect.getInspectMethod() == null) {
			inspect.setInspectMethod("row_count");
		}

		for (int i = 0; i < inspect.getTasks().size(); i++) {
			com.tapdata.entity.inspect.InspectTask task = inspect.getTasks().get(i);
			if (task == null) {
				logger.warn("Inspect.tasks[" + i + "] is empty.");
				continue;
			}
			if (StringUtils.isEmpty(task.getTaskId())) {
				errorMsg.add("Inspect.tasks[" + i + "].taskId can not be empty");
			}
			checkFieldInspectTaskDataSource("Inspect.tasks[" + i + "].source", task.getSource());
			checkFieldInspectTaskDataSource("Inspect.tasks[" + i + "].target", task.getTarget());
		}

		return errorMsg;

	}

	private List<String> checkFieldInspectTaskDataSource(String prefix, InspectDataSource dataSource) {
		List<String> errorMsg = new ArrayList<>();
		if (StringUtils.isEmpty(dataSource.getConnectionId())) {
			errorMsg.add(prefix + ".connectionId can not be empty.");
		}
		if (StringUtils.isEmpty(dataSource.getTable())) {
			errorMsg.add(prefix + ".table can not be empty.");
		}
		if (StringUtils.isEmpty(dataSource.getDirection())) {
			dataSource.setDirection("DESC");
		}
		if (StringUtils.isEmpty(dataSource.getSortColumn())) {
			errorMsg.add(prefix + ".sortColumn can not be empty.");
		}
		return errorMsg;
	}

	public void inspectHeartBeat(String id) {
		Query query = new Query(Criteria.where("_id").is(id));
		Update update = new Update();
		update.set("ping_time", System.currentTimeMillis());
		clientMongoOperator.update(query, update, ConnectorConstant.INSPECT_COLLECTION);
	}

	public SettingService getSettingService() {
		return settingService;
	}
}
