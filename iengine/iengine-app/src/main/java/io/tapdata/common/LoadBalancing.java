package io.tapdata.common;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Worker;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Calendar;
import java.util.List;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class LoadBalancing {

	private boolean needBalancing;
	private String mode;
	private String agentId;
	private String workType;
	private ClientMongoOperator clientMongoOperator;
	public static volatile int runningThreadNum;

	private static final long BALANCING_SLEEP_MS = 3000;
	private static final String SINGLETON = "singleton";
	private static final String CLUSTER = "cluster";

	public LoadBalancing() {
	}

	public LoadBalancing(String mode, String agentId, String workType, ClientMongoOperator clientMongoOperator) {
		this.mode = mode;
		this.agentId = agentId;
		this.workType = workType;
		this.clientMongoOperator = clientMongoOperator;
		runningThreadNum = 0;
		needBalancing = true;
	}

	public boolean balancing() throws InterruptedException {
		boolean isBalancing = false;
		if (mode.equals(CLUSTER)) {
			if (needBalancing && isMostJobWorker()) {
				Thread.sleep(BALANCING_SLEEP_MS);
				isBalancing = true;
			}
			needBalancing = !needBalancing;
		}
		return isBalancing;
	}

	private boolean isMostJobWorker() {
		boolean isMost = true;

		Criteria typeWhere = new Criteria("worker_type").is(workType);
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MINUTE, -1);
		Criteria pingTimeWhere = new Criteria().andOperator(
				where("ping_time").exists(true),
				where("ping_time").ne(null),
				where("ping_time").gte(Double.valueOf(String.valueOf(calendar.getTimeInMillis())))
		);
		Query query = new Query(new Criteria().andOperator(
				typeWhere,
				pingTimeWhere
		));
		List<Worker> workers = clientMongoOperator.find(query, ConnectorConstant.WORKER_COLLECTION, Worker.class);
		if (CollectionUtils.isNotEmpty(workers)) {

			for (Worker worker : workers) {
				if (worker.getRunning_thread() > runningThreadNum) {
					isMost = false;
					break;
				}
			}
		}

		return isMost;
	}
}
