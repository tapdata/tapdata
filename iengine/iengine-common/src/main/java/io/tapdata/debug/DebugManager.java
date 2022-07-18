package io.tapdata.debug;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Job;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.List;

public class DebugManager {

	private final static Logger logger = LogManager.getLogger(DebugManager.class);

	private List<Job> jobs;
	private ClientMongoOperator clientMongoOperator;

	public DebugManager(List<Job> jobs, ClientMongoOperator clientMongoOperator) {
		this.jobs = jobs;
		this.clientMongoOperator = clientMongoOperator;
	}

	public void startDebugJobs() {
		if (CollectionUtils.isNotEmpty(jobs)) {
			Integer order = 1;
			Job previousJob = null;

			try {
				DebugProcessor.clearDebugs(jobs.get(0).getDataFlowId(), clientMongoOperator);
			} catch (DebugException e) {
				logger.error("Clear debug data error, message: {}", e.getMessage(), e);
			}

			for (Job job : jobs) {

				Document filter = new Document("_id", job.getId());
				Document update = new Document("status", ConnectorConstant.SCHEDULED)
						.append("debug_order", order);
				if (order > 1) {
					update.append("previous_job", new Document("id", previousJob.getId()).append("mappings", previousJob.getMappings()));
				}

				if (StringUtils.equalsAny(job.getStatus(), ConnectorConstant.STOPPING, ConnectorConstant.FORCE_STOPPING, ConnectorConstant.ERROR)) {
					continue;
				}

				// start job one by one
				clientMongoOperator.updateAndParam(filter, update, "Jobs");
				order++;
				previousJob = job;
				job.setStatus(ConnectorConstant.SCHEDULED);

				while (true) {
					if (StringUtils.equalsAny(job.getStatus(), ConnectorConstant.STOPPING, ConnectorConstant.FORCE_STOPPING, ConnectorConstant.ERROR, ConnectorConstant.PAUSED)) {
						break;
					}
					try {
						Thread.sleep(3000l);
					} catch (InterruptedException e) {
						logger.info("Debug interrupted.");
					}
				}
			}

			// 预览结束后，excuteMode改为normal
			/*if (StringUtils.isNotBlank(jobs.get(0).getDataFlowId())) {
				UpdateResult updateResult = clientMongoOperator.updateAndParam(new Document("_id", jobs.get(0).getDataFlowId()),
						new Document("executeMode", DataFlow.EXECUTEMODE_NORMAL),
						"DataFlows");
			}*/
		}
	}
}
