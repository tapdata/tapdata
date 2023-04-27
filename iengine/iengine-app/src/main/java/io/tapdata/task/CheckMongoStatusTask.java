package io.tapdata.task;

import com.mongodb.MongoClientURI;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.Worker;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.entity.BaseConnectionValidateResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

@TaskType(type = "CHECK_MONGODB_STATUS")
public class CheckMongoStatusTask implements Task {

	private final static Logger logger = LogManager.getLogger(CheckMongoStatusTask.class);
	private TaskContext context;
	private List<String> testedHosts = new ArrayList<>();
	private final static String AUTH_SOURCE = "admin";
	private final static String SERVER_SELECTION_TIMEOUT_MS = "5000";

	private ExecutorService checkPools;
	private Lock lock = new ReentrantLock(true);

	@Override
	public void initialize(TaskContext taskContext) {
		this.context = taskContext;
	}

	@Override
	public void execute(Consumer<TaskResult> callback) {
		TaskResult result = new TaskResult();

		try {
			ClientMongoOperator clientMongoOperator = context.getClientMongoOperator();
			Map<String, Integer> checkMap = new HashMap<>();

			List<Connections> connectionsList = getTargetReadyMongodbConnections();

			if (CollectionUtils.isNotEmpty(connectionsList)) {
				checkPools = Executors.newCachedThreadPool();

				for (Connections connections : connectionsList) {

					List<String> mongoUris = getMongoUris(connections);

					for (String curUri : mongoUris) {

						Runnable runnable = () -> {
							int i = StringUtils.lastIndexOf(curUri, "/");
							String uri = "";
							if (i >= 0) {
								uri = curUri.substring(0, i);
							}

							if (checkIfNeedTest(uri)) {

								lock.lock();
								try {
									testedHosts.add(uri);
								} finally {
									lock.unlock();
								}

								Map<String, String> hostPortMap = MongodbUtil.getHostPortMap(uri);
								String host = hostPortMap.get("hosts");
								String port = hostPortMap.get("ports");
								host = StringUtils.isNoneBlank(host) ? host : "-";
								port = StringUtils.isNoneBlank(port) ? port : "-";

								logger.info("Starting check mongodb status[host: {}, port: {}]", host, port);
								int isActive = MongodbUtil.checkTargetMongodbIsActive(curUri);
								logger.info("Finished check mongodb status[host: {}, port: {}], result: {}",
										host, port,
										isActive == 1 ? "active" : "inactive");


								Map<String, Object> queryMap = new HashMap<>();
								queryMap.put("worker_ip", host);
								queryMap.put("port", port);
								queryMap.put("worker_type", ConnectorConstant.WORKER_TARGET_MONGODB_STATUS);

								Map<String, Object> updateMap = new HashMap<>();
								updateMap.put("worker_type", ConnectorConstant.WORKER_TARGET_MONGODB_STATUS);
								updateMap.put("worker_ip", host);
								updateMap.put("port", port);
								updateMap.put("hostname", host.replace('.', '_') + ":" + port);
								updateMap.put("status", isActive);
								updateMap.put("process_id", host + ":" + port);
								updateMap.put("mongodb_uri", uri);

								clientMongoOperator.upsert(queryMap, updateMap, ConnectorConstant.WORKER_COLLECTION);

								checkMap.put(host.replace('.', '_') + ":" + port, isActive);
							}
						};

						checkPools.submit(runnable);
					}
				}

				deleteNoUseMongoUri();

				checkPools.shutdown();
				try {
					if (!checkPools.awaitTermination(10, TimeUnit.SECONDS)) {
						checkPools.shutdownNow();
					}
				} catch (InterruptedException e) {
					checkPools.shutdownNow();
				}
			}

			result.setTaskResultCode(200);
			result.setTaskResult(checkMap);
		} catch (Exception e) {
			result.setTaskResultCode(201);
			result.setTaskResult(String.format("Failed to check target mongodb status, message: %s.", e.getMessage()));
		}
		testedHosts.clear();
		callback.accept(result);
	}

	private List<Connections> getTargetReadyMongodbConnections() {
		Criteria mongodb = new Criteria().andOperator(
				Criteria.where("database_type").is(DatabaseTypeEnum.MONGODB.getType()),
				Criteria.where("status").is(BaseConnectionValidateResult.CONNECTION_STATUS_READY),
				new Criteria().orOperator(
						Criteria.where("connection_type").is(ConnectorConstant.CONNECTION_TYPE_TARGET),
						Criteria.where("connection_type").is(ConnectorConstant.CONNECTION_TYPE_SOURCE_TARGET)
				)
		);

		Criteria bitsflowSource = new Criteria().andOperator(
				Criteria.where("database_type").is(DatabaseTypeEnum.BITSFLOW.getType()),
				Criteria.where("status").is(BaseConnectionValidateResult.CONNECTION_STATUS_READY),
				new Criteria().orOperator(
						Criteria.where("connection_type").is(ConnectorConstant.CONNECTION_TYPE_SOURCE),
						Criteria.where("connection_type").is(ConnectorConstant.CONNECTION_TYPE_SOURCE_TARGET)
				)
		);

		Criteria gridfs = new Criteria().andOperator(
				Criteria.where("database_type").is(DatabaseTypeEnum.GRIDFS.getType()),
				Criteria.where("status").is(BaseConnectionValidateResult.CONNECTION_STATUS_READY)
		);

		Query query = new Query(new Criteria().orOperator(
				mongodb,
				bitsflowSource,
				gridfs
		));

		query.fields().exclude("schema");

		return context.getClientMongoOperator().find(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
	}

	private boolean checkIfNeedTest(String uri) {
		lock.lock();
		try {
			if (StringUtils.isBlank(uri)) {
				return false;
			} else {
				if (CollectionUtils.isEmpty(testedHosts)) {
					return true;
				} else {
					for (String testedHost : testedHosts) {
						if (testedHost.equals(uri)) {
							return false;
						}
					}
					return true;
				}
			}
		} finally {
			lock.unlock();
		}
	}

	private static List<String> getMongoUris(Connections connections) {
		List<String> list = new ArrayList<>();
		List<String> hostList = new ArrayList<>();

		if (connections == null) {
			return list;
		}
		String databaseUri = connections.getDatabase_uri();
		MongoClientURI tempUri = new MongoClientURI(databaseUri);
		String username = tempUri.getUsername();
		char[] password = tempUri.getPassword();
		List<String> before = tempUri.getHosts();

		// get uris from connections uri
		for (int i = 0; i < before.size(); i++) {
			String tempHost = before.get(i);
			if (tempHost.equalsIgnoreCase("localhost")
					|| tempHost.equalsIgnoreCase("localhost:27017")) {
				tempHost = "127.0.0.1:27017";
			}

			hostList.add(tempHost);
		}

		// get shard uris
		try {
			Map<String, String> hostMap = MongodbUtil.nodesURI(connections, SERVER_SELECTION_TIMEOUT_MS);
			for (Map.Entry<String, String> entry : hostMap.entrySet()) {
				String value = entry.getValue();

				MongoClientURI mongoClientURI = new MongoClientURI(value);

				List<String> hosts = mongoClientURI.getHosts();

				for (String host : hosts) {
					hostList.add(host);
				}
			}
		} catch (Exception e) {
			// do nothing
		}

		// get config server uris if sharding
		try {
			List<String> configServerHosts = MongodbUtil.getConfigServerHosts(connections.getDatabase_uri());
			for (String configServerHost : configServerHosts) {
				hostList.add(configServerHost);
			}
		} catch (Exception e) {
			// do nothing
		}

		// distinct
		LinkedHashSet<String> linkedHashSet = parseHosts(hostList);
		hostList.clear();
		hostList.addAll(linkedHashSet);

		for (String host : hostList) {
			list.add(MongodbUtil.appendMongoUri(host, username, password, SERVER_SELECTION_TIMEOUT_MS, AUTH_SOURCE));
		}

		return list;
	}

	private static LinkedHashSet<String> parseHosts(List<String> list) {
		LinkedHashSet<String> linkedHashSet = new LinkedHashSet<>(list.size());
		for (int i = 0; i < list.size(); i++) {
			String ipStr = list.get(i);

			if (StringUtils.isNotBlank(ipStr)) {
				try {
					String[] split = ipStr.split(":");
					String host = split[0];
					String port = split.length > 1 ? split[1] : "27017";
					InetAddress inetAddress = InetAddress.getByName(host);

					ipStr = inetAddress.getHostAddress() + ":" + port;

				} catch (UnknownHostException e) {
					logger.error("InetAddress get by name error: {}", e.getMessage(), e);
				}

				linkedHashSet.add(ipStr);
			}
		}

		return linkedHashSet;
	}

	private List<Worker> getWorkerMongoDB() {
		Query query = new Query(
				new Criteria().andOperator(
						new Criteria("mongodb_uri").nin(testedHosts),
						new Criteria("worker_type").is("MongoDB")
				)
		);


		return context.getClientMongoOperator().find(query, ConnectorConstant.WORKER_COLLECTION, Worker.class);
	}

	private void deleteNoUseMongoUri() {
		List<Worker> deleteWorkerMongoDB = getWorkerMongoDB();
		Map<String, Object> params = new HashMap<>();
		for (Worker worker : deleteWorkerMongoDB) {
			params.put("_id", worker.getId());
			context.getClientMongoOperator().delete(params, ConnectorConstant.WORKER_COLLECTION);
		}

	}
}
