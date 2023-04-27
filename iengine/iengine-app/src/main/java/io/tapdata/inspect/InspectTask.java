package io.tapdata.inspect;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ExecutorUtil;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.inspect.Inspect;
import com.tapdata.entity.inspect.InspectDetail;
import com.tapdata.entity.inspect.InspectMethod;
import com.tapdata.entity.inspect.InspectResult;
import com.tapdata.entity.inspect.InspectResultStats;
import com.tapdata.entity.inspect.InspectStatus;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.log.LogFactory;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.inspect.cdc.InspectCdcUtils;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.ConnectionTapTableMap;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/10 7:19 上午
 * @description
 */
public abstract class InspectTask implements Runnable {

	private final Inspect inspect;
	private final InspectService inspectService;
	private ClientMongoOperator clientMongoOperator;
	private Logger logger = LogManager.getLogger(InspectTask.class);
	private static final String INSPECT_THREAD_NAME_PREFIX = "INSPECT-RUNNER-";
	private static final String PROCESS_ID = ConfigurationCenter.processId;

	public InspectTask(InspectService inspectService, Inspect inspect, ClientMongoOperator clientMongoOperator) {
		this.inspectService = inspectService;
		this.inspect = inspect;
		this.clientMongoOperator = clientMongoOperator;
	}

	@Override
	public void run() {
		String name = INSPECT_THREAD_NAME_PREFIX + inspect.getName() + "(" + inspect.getId() + ")";
		Thread.currentThread().setName(name);

		InspectResult _inspectResult = null;
		if (StringUtils.isNotEmpty(inspect.getInspectResultId())) {
			_inspectResult = inspectService.getInspectResultById(inspect.getInspectResultId());

			if (_inspectResult == null) {
				logger.info("Not found inspect result on restart inspect task: {}({}, {})",
						inspect.getName(), inspect.getId(), inspect.getInspectResultId());

				inspectService.updateStatus(inspect.getId(), InspectStatus.ERROR, "Not found inspect result by id: "
						+ inspect.getInspectResultId());

				return;
			}
			_inspectResult.setPartStats(true);
		}
		InspectResult inspectResult = _inspectResult != null ? _inspectResult : new InspectResult();
		Map<String, ConnectorNode> connectorNodeMap = null;
		try {
			logger.info("Start execute data inspect: {}({})", inspect.getName(), inspect.getId());

			inspectService.updateStatus(inspect.getId(), InspectStatus.RUNNING, null);

			List<Connections> connections = inspectService.getInspectConnectionsById(inspect);
			Map<String, Connections> connectionsMap = new HashMap<>();
			connections.forEach(connection -> connectionsMap.put(connection.getId(), connection));
			connectorNodeMap = initConnectorNodeMap(connectionsMap);

			// Parallel check of multiple tables
			ThreadPoolExecutor executorService = new ThreadPoolExecutor(0, 10, 0,
					TimeUnit.SECONDS, new LinkedBlockingQueue<>());

			inspectResult.setInspect_id(inspect.getId());
			inspectResult.setInspectVersion(inspect.getVersion());
			inspectResult.setStats(Collections.synchronizedList(new ArrayList<>()));
			inspectResult.setThreads(5);
			inspectResult.setStatus("running");
			inspectResult.setProgress(0);
			inspectResult.setStart(new Date());
			inspectResult.setAgentId(PROCESS_ID);
			inspectResult.setCustomId(inspect.getCustomId());
			inspectResult.setUser_id(inspect.getUser_id());

			// 如果是差异校验，firstCheckId 有值
			String firstCheckId = inspect.getByFirstCheckId();
			if (null != firstCheckId && !firstCheckId.isEmpty()) {
				String errorMsg = null;
				InspectResult lastInspectResult = null;
				if (InspectMethod.CDC_COUNT.equalsString(inspect.getInspectMethod())) {
					errorMsg = "Nonsupport difference inspect by method: '" + inspect.getInspectMethod() + "'";
				} else {
					// 取最后一次有差异结果
					lastInspectResult = inspectService.getLastDifferenceInspectResult(firstCheckId);
					inspectResult.setFirstCheckId(firstCheckId);
					if (null == lastInspectResult) {
						errorMsg = "Not found last difference inspect result in '" + firstCheckId + "'";
					}
				}
				if (null != errorMsg) {
					inspectResult.setInspect(inspect);
					inspectResult.setEnd(new Date());
					inspectResult.setStatus(InspectStatus.ERROR.getCode());
					inspectResult.setErrorMsg(errorMsg);
					inspectResult.setStats(Collections.singletonList(new InspectResultStats()));
					inspectService.upsertInspectResult(inspectResult, false);
					throw new Exception(errorMsg);
				}
				inspectResult.setInspect(lastInspectResult.getInspect());
				inspectResult.getInspect().getTasks().forEach(task -> {
					InspectResultStats stats = new InspectResultStats();

					stats.setStart(new Date());
					stats.setStatus("running");
					stats.setProgress(0);
					stats.setTaskId(task.getTaskId());
					stats.setSource(task.getSource());
					stats.setTarget(task.getTarget());

					inspectResult.getStats().add(stats);
				});
				inspectResult.setParentId(lastInspectResult.getId());
				inspectResult.setFirstSourceTotal(lastInspectResult.getFirstSourceTotal());
				inspectResult.setFirstTargetTotal(lastInspectResult.getFirstTargetTotal());
				for (int i = 0, len1 = inspectResult.getStats().size(), len2 = lastInspectResult.getStats().size(); i < len1 && i < len2; i++) {
					inspectResult.getStats().get(i).setFirstSourceTotal(lastInspectResult.getStats().get(i).getFirstSourceTotal());
					inspectResult.getStats().get(i).setFirstTargetTotal(lastInspectResult.getStats().get(i).getFirstTargetTotal());
				}

				inspectService.upsertInspectResult(inspectResult, false);
			} else {
				inspectResult.setInspect(inspect);
				inspectResult.getInspect().getTasks().forEach(task -> {
					InspectResultStats stats = new InspectResultStats();

					stats.setStart(new Date());
					stats.setStatus("running");
					stats.setProgress(0);
					stats.setTaskId(task.getTaskId());
					stats.setSource(task.getSource());
					stats.setTarget(task.getTarget());

					inspectResult.getStats().add(stats);
				});
				inspectService.upsertInspectResult(inspectResult, false);
				InspectCdcUtils.setCdcRunProfilesByLastResult(inspectService, inspectResult); // 填充增量运行配置

				// 初次校验，取当前结果编号
				inspectResult.setFirstCheckId(inspectResult.getId());
				inspectService.upsertInspectResult(inspectResult, false);
			}

			List<InspectDetail> inspectDetailQueue = new ArrayList<>();

			// async report inspect stats
			final Object lock = new Object();
			Thread reportThread = new Thread(() -> {
				while (!Thread.currentThread().isInterrupted()) {
					inspectService.upsertInspectResult(inspectResult);
					List<InspectDetail> temp;
					synchronized (inspectDetailQueue) {
						temp = new ArrayList<>(inspectDetailQueue);
						inspectDetailQueue.clear();
					}
					inspectService.insertInspectDetails(temp);

					synchronized (lock) {
						try {
							lock.wait();
						} catch (InterruptedException e) {
							logger.info("Interrupted data inspect async report thread");
							break;
						}
					}
				}
			});
			reportThread.setName(name + ".reportThread");
			reportThread.start();

			ScheduledExecutorService heartBeatThreads = new ScheduledThreadPoolExecutor(1);
			heartBeatThreads.scheduleAtFixedRate(() -> inspectService.inspectHeartBeat(inspect.getId()), 5, 5, TimeUnit.SECONDS);

			List<Future<?>> futures = new ArrayList<>();

			// Create a verification task for each table and submit it for execution
			Map<String, ConnectorNode> finalConnectorNodeMap = connectorNodeMap;
			inspectResult.getInspect().getTasks().forEach(task -> {

				task.setLimit(inspectResult.getInspect().getLimit());

				Connections source = connectionsMap.get(task.getSource().getConnectionId()),
						target = connectionsMap.get(task.getTarget().getConnectionId());
				if (source == null) {
					logger.error("Not found source Connections by connectionId " + task.getSource().getConnectionId());
					return;
				}
				if (target == null) {
					logger.error("Not found target Connections by connectionId " + task.getTarget().getConnectionId());
					return;
				}
				ConnectorNode sourceNode = finalConnectorNodeMap.get(task.getSource().getConnectionId()),
						targetNode = finalConnectorNodeMap.get(task.getTarget().getConnectionId());

				AtomicLong atomicLong = new AtomicLong(System.currentTimeMillis());
				// submit verification task
				InspectTaskContext inspectTaskContext = new InspectTaskContext(
						name,
						task,
						source,
						target,
						inspectResult.getParentId(),
						inspect.getInspectDifferenceMode(),
						(inspectTask, inspectResultStats, inspectDetails) -> {

							logger.info(inspectTask.getTaskId() + " inspect done, status " + inspectResultStats.getStatus() + ", result " + inspectResultStats.getResult());

							Optional<InspectResultStats> optional = inspectResult.getStats().stream()
									.filter((stats) -> stats.getTaskId().equals(inspectResultStats.getTaskId()))
									.findFirst();
							if (optional.isPresent()) {
								InspectResultStats existsStats = optional.get();
								if (existsStats.getStatus().equals("error")) {
									existsStats.setResult("");
								}
								if (existsStats != inspectResultStats) {
									inspectResultStats.setFirstSourceTotal(existsStats.getFirstSourceTotal());
									inspectResultStats.setFirstTargetTotal(existsStats.getFirstTargetTotal());
									inspectResult.getStats().remove(existsStats);
									inspectResult.getStats().add(inspectResultStats);
								}
							} else {
								inspectResult.getStats().add(inspectResultStats);
							}

							composeInspectResult(inspectResult);
							inspectResult.setThreads(executorService.getActiveCount());

							// async report stats and progress
							long current = System.currentTimeMillis();
							boolean notify = false;
							if ((current - atomicLong.get()) > 5000) {
								atomicLong.set(current);
								notify = true;
							}
							if (inspectDetails != null && inspectDetails.size() > 0) {
								inspectDetails.forEach(detail -> {
									detail.setInspect_id(inspect.getId());
									detail.setTaskId(inspectTask.getTaskId());
									detail.setInspectResultId(inspectResult.getId());
									detail.setUser_id(inspect.getUser_id());
									detail.setCustomId(inspect.getCustomId());
								});
								synchronized (inspectDetailQueue) {
									inspectDetailQueue.addAll(inspectDetails);
								}
								notify = true;
							}
							if (notify) {
								synchronized (lock) {
									lock.notify();
								}
							}
						}, sourceNode, targetNode, clientMongoOperator
				);
				futures.add(executorService.submit(createTableInspectJob(inspectTaskContext)));
			});

			boolean hasError = false;
			String errorMessage = "";
			for (Future future : futures) {
				try {
					future.get();
				} catch (InterruptedException | ExecutionException e) {
					logger.error("Execute data inspect failed", e);
					hasError = true;
					errorMessage += e.getMessage() + ";";
				}
			}

			reportThread.interrupt();
			ExecutorUtil.shutdown(executorService, 10L, TimeUnit.SECONDS);
			ExecutorUtil.shutdown(heartBeatThreads, 10L, TimeUnit.SECONDS);
			inspectResult.setEnd(new Date());

			inspectService.upsertInspectResult(inspectResult);
			synchronized (inspectDetailQueue) {
				inspectService.insertInspectDetails(inspectDetailQueue);
			}

			if (InspectStatus.ERROR.getCode().equals(inspectResult.getStatus())) {
				hasError = true;
				errorMessage = inspectResult.getErrorMsg();
			}

			InspectStatus inspectStatus = null;
			if (hasError) {
				inspectStatus = InspectStatus.ERROR;
			} else {
				switch (Inspect.Mode.fromValue(inspect.getMode())) {
					case CRON:
						inspectStatus = InspectStatus.WAITING;
						break;
					case MANUAL:
						inspectStatus = InspectStatus.DONE;
						break;
					default:
						break;
				}
			}
			String msg = errorMessage;
			Optional.ofNullable(inspectStatus).ifPresent(status -> inspectService.updateStatus(inspect.getId(), status, msg));
			logger.info("Execute data verification done.");
		} catch (Throwable e) {
			logger.error("Execute data verification failed", e);
			if (null == inspectResult.getErrorMsg()) {
				inspectResult.setErrorMsg("Execute data verification error: " + e.getMessage());
			}
			inspectService.updateStatus(inspect.getId(), InspectStatus.ERROR, "Execute data verification error: " + e.getMessage());
		} finally {
			releaseConnectionNodes(connectorNodeMap);
			inspectService.stopInspect(inspect);
		}
	}

	private Map<String, ConnectorNode> initConnectorNodeMap(Map<String, Connections> connectionsMap) {
		Map<String, ConnectorNode> connectorNodeMap = new HashMap<>();
		for (com.tapdata.entity.inspect.InspectTask task : inspect.getTasks()) {
			Connections sourceConn = connectionsMap.get(task.getSource().getConnectionId());
			Connections targetConn = connectionsMap.get(task.getTarget().getConnectionId());
			if (!connectorNodeMap.containsKey(task.getSource().getConnectionId())) {
				connectorNodeMap.put(sourceConn.getId(), initConnectorNode(task.getSource().getNodeId(), sourceConn));
			}
			if (!connectorNodeMap.containsKey(task.getTarget().getConnectionId())) {
				connectorNodeMap.put(targetConn.getId(), initConnectorNode(task.getTarget().getNodeId(), targetConn));
			}
		}
		return connectorNodeMap;
	}

	private ConnectorNode initConnectorNode(String nodeId, Connections connection) {
		DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connection.getPdkHash());
		String associateId = InspectTask.class.getSimpleName() + "-" + UUID.randomUUID();
		KVReadOnlyMap<TapTable> tapTableMap;
		if (null == nodeId || nodeId.isEmpty()) {
			associateId += "-conn-" + connection.getId();
			tapTableMap = new ConnectionTapTableMap(connection.getId(), connection.getName());
		} else {
			associateId += "-node-" + nodeId;
			tapTableMap = new PdkTableMap(TapTableUtil.getTapTableMapByNodeId(nodeId));
		}

		return PdkUtil.createNode(
				inspect.getFlowId(),
				databaseType,
				clientMongoOperator,
				associateId,
				connection.getConfig(),
				tapTableMap,
				new PdkStateMap(connection.getId(), HazelcastUtil.getInstance()),
				PdkStateMap.globalStateMap(HazelcastUtil.getInstance()),
				InstanceFactory.instance(LogFactory.class).getLog()
		);
	}

	private void releaseConnectionNodes(Map<String, ConnectorNode> connectorNodeMap) {
		if (MapUtils.isEmpty(connectorNodeMap)) return;
		for (ConnectorNode connectorNode : connectorNodeMap.values()) {
			if (null == connectorNode || StringUtils.isBlank(connectorNode.getAssociateId()))
				continue;
			PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, this.getClass().getSimpleName());
			PDKIntegration.releaseAssociateId(connectorNode.getAssociateId());
		}
	}

	/**
	 * create a table Inspect job
	 *
	 * @param inspectTaskContext
	 * @return
	 * @name
	 */
	public abstract Runnable createTableInspectJob(InspectTaskContext inspectTaskContext);

	/**
	 * compose inspect result
	 *
	 * @param inspectResult
	 */
	private void composeInspectResult(InspectResult inspectResult) {

		if (inspectResult.getStats() != null) {

			AtomicBoolean isRunning = new AtomicBoolean(false);
			AtomicBoolean hasError = new AtomicBoolean(false);
			AtomicReference<Double> progressSum = new AtomicReference<>((double) 0);
			AtomicLong firstSourceTotal = new AtomicLong(0);
			AtomicLong firstTargetTotal = new AtomicLong(0);
			AtomicLong sourceTotal = new AtomicLong(0);
			AtomicLong targetTotal = new AtomicLong(0);
			StringBuilder sb = new StringBuilder();

			for (int i = 0; i < inspectResult.getStats().size(); i++) {
				InspectResultStats stats = inspectResult.getStats().get(i);
				if ("running".equalsIgnoreCase(stats.getStatus())) {
					isRunning.set(true);
				}
				if ("error".equalsIgnoreCase(stats.getStatus())) {
					hasError.set(true);
					sb.append(stats.getErrorMsg());
				}
				if (inspectResult.getId().equals(inspectResult.getFirstCheckId())) {
					stats.setFirstSourceTotal(stats.getSource_total());
					stats.setFirstTargetTotal(stats.getTarget_total());
				}
				progressSum.updateAndGet(v -> v + stats.getProgress());
				firstSourceTotal.updateAndGet(v -> v + stats.getFirstSourceTotal());
				firstTargetTotal.updateAndGet(v -> v + stats.getFirstTargetTotal());
				sourceTotal.updateAndGet(v -> v + stats.getSource_total());
				targetTotal.updateAndGet(v -> v + stats.getTarget_total());
			}

			inspectResult.setStatus(hasError.get() ? "error" : isRunning.get() ? "running" : "done");
			inspectResult.setErrorMsg(hasError.get() ? sb.toString() : null);
			inspectResult.setFirstSourceTotal(firstSourceTotal.get());
			inspectResult.setFirstTargetTotal(firstTargetTotal.get());
			inspectResult.setSource_total(sourceTotal.get());
			inspectResult.setTarget_total(targetTotal.get());
			inspectResult.setProgress(progressSum.get() / inspectResult.getStats().size());
		}

	}
}
