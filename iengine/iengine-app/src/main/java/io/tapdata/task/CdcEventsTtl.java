package io.tapdata.task;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2020-09-07 23:15
 **/
@TaskType(type = "CDC_EVENTS_TTL")
public class CdcEventsTtl implements Task {

	private Logger logger = LogManager.getLogger(CdcEventsTtl.class);

	private TaskContext taskContext;

	@Override
	public void initialize(TaskContext taskContext) {
		this.taskContext = taskContext;
	}

	@Override
	public void execute(Consumer<TaskResult> callback) {
		try {
			ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();
			if (clientMongoOperator == null) {
				return;
			}

			long currentTimeMillis = System.currentTimeMillis();
			int jobCdcRecordTtlDoc = Integer.valueOf(taskContext.getSettingService().getString("job_cdc_record_ttl", "7"));
			long ttlMills = currentTimeMillis - (jobCdcRecordTtlDoc * 24 * 60 * 60 * 1000L);

			Map<String, Object> params = new HashMap<>();
			params.put("insertDate", new HashMap<String, Object>() {{
				put("$lt", Double.valueOf(ttlMills));
			}});

			clientMongoOperator.deleteAll(params, ConnectorConstant.CDC_EVENTS_COLLECTION);
		} catch (Exception e) {
			logger.error("Clear cdc events error, err msg: {}", e.getMessage(), e);
		}
	}
}
