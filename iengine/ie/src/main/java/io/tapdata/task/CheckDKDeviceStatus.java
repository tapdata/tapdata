package io.tapdata.task;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Worker;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.SettingService;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@TaskType(type = "CHECK_DK_DEVICE_PING")
public class CheckDKDeviceStatus implements Task {

	private final static int DEFAULT_TIMEOUT = 3000;
	private final static Logger logger = LogManager.getLogger(CheckDKDeviceStatus.class);
	private final static String WORKER_TYPE = "Device";

	private TaskContext context;
	private String ipStrs;
	private String[] ipAddresses;
	private int timeout;
	private List<String> testedIp;
	private ExecutorService checkPools;

	@Override
	public void initialize(TaskContext taskContext) {
		this.context = taskContext;
	}

	@Override
	public void execute(Consumer<TaskResult> callback) {
		testedIp = new ArrayList<>();
		SettingService settingService = context.getSettingService();
		this.ipStrs = settingService.getString("dkcheck.ipaddresses", "");
		if (StringUtils.isNotBlank(ipStrs)) {
			ipStrs = ipStrs.replaceAll(" ", "").replaceAll("/", "").replaceAll("\\\\", "");
			this.ipAddresses = ipStrs.split(",");
		}
		this.timeout = settingService.getInt("dkcheck.timeout", DEFAULT_TIMEOUT);

		TaskResult taskResult = new TaskResult();
		Map<String, Boolean> resultMap = new HashMap<>();
		Map<String, String> errorMap = new HashMap<>();
		ClientMongoOperator clientMongoOperator = context.getClientMongoOperator();

		if (ipAddresses != null && ipAddresses.length > 0) {
			logger.info("Starting check devices\n    ip addresses: {}\n    timeout: {}(ms)", ipStrs, timeout);

			checkPools = Executors.newCachedThreadPool();

			for (String ipAddress : ipAddresses) {

				Runnable runnable = () -> {
					try {
						logger.info("Starting check device, ip address: {}", ipAddress);
						boolean reachable = ping(ipAddress, timeout);
						logger.info("Finished check device reachable, ip address: {}, result: {}", ipAddress, reachable);

						Document query = new Document();
						query.append("worker_type", WORKER_TYPE);
						query.append("process_id", "device_" + ipAddress);

						Document update = new Document();
						update.append("worker_ip", ipAddress);
						update.append("worker_type", WORKER_TYPE);
						update.append("status", reachable ? 1 : 0);
						update.append("timeout", timeout);
						update.append("process_id", "device_" + ipAddress);
						update.append("hostname", reachable ? InetAddress.getByName(ipAddress).getHostName() : ipAddress.replace('.', '_'));

						clientMongoOperator.upsert(query, update, ConnectorConstant.WORKER_COLLECTION);

						resultMap.put(ipAddress, reachable);
						testedIp.add(ipAddress);
					} catch (Exception e) {
						logger.error("Check device error, ip address: {}, message: {}", ipAddress, e.getMessage(), e);
						resultMap.put(ipAddress, false);
						errorMap.put(ipAddress, e.getMessage());
					}
				};

				checkPools.submit(runnable);
			}

			deleteNoUseDeviceIp();

			checkPools.shutdown();
			try {
				if (!checkPools.awaitTermination(timeout + 1000, TimeUnit.MILLISECONDS)) {
					checkPools.shutdownNow();
				}
			} catch (InterruptedException e) {
				checkPools.shutdownNow();
			}

			taskResult.setTaskResultCode(200);
			taskResult.setTaskResult(ipAddresses);
			taskResult.setFailedResult(errorMap);
		} else {
			taskResult.setTaskResultCode(201);
			taskResult.setFailedResult("Found no ip addresses in settings");
		}

		callback.accept(taskResult);
	}

	public static boolean ping(String ipAddress, int timeout) throws IOException {
		boolean reachable = InetAddress.getByName(ipAddress).isReachable(timeout);
		return reachable;
	}

	private List<Worker> getWorkerDevice() {
		Query query = new Query(
				new Criteria().andOperator(
						new Criteria("worker_ip").nin(testedIp),
						new Criteria("worker_type").is(WORKER_TYPE)
				)
		);

		return context.getClientMongoOperator().find(query, ConnectorConstant.WORKER_COLLECTION, Worker.class);
	}

	private void deleteNoUseDeviceIp() {
		List<Worker> workerDevice = getWorkerDevice();
		Map<String, Object> params = new HashMap<>();
		for (Worker worker : workerDevice) {
			params.put("_id", worker.getId());
			context.getClientMongoOperator().delete(params, ConnectorConstant.WORKER_COLLECTION);
		}
	}
}
