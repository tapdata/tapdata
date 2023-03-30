package io.tapdata.observable.logging.appender;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.tapdata.constant.JSONUtil;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.filter.BurstFilter;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2023-03-08 15:40
 **/
public class ObsHttpTMAppender extends BaseTaskAppender<MonitoringLogsDto> {

	private static final long serialVersionUID = -1887785881704714112L;
	public static final String LOGGER_NAME_PREFIX = "job-http-tm-log-";
	private final Logger rootLogger = LogManager.getLogger(ObsHttpTMAppender.class);
	private final Logger logger;
	private ObsHttpTMLog4jAppender obsHttpTMLog4jAppender;
	private final ClientMongoOperator clientMongoOperator;

	private ObsHttpTMAppender(ClientMongoOperator clientMongoOperator, String taskId) {
		super(taskId);
		this.clientMongoOperator = clientMongoOperator;
		this.logger = LogManager.getLogger(LOGGER_NAME_PREFIX + taskId);
	}

	public static ObsHttpTMAppender create(ClientMongoOperator clientMongoOperator, String taskId) {
		return new ObsHttpTMAppender(clientMongoOperator, taskId);
	}

	@Override
	public void start() {
		this.obsHttpTMLog4jAppender = ObsHttpTMLog4jAppender.createAppender(String.join("-", ObsHttpTMLog4jAppender.class.getSimpleName(), taskId),
				null, null, false, null, clientMongoOperator, AppenderFactory.BATCH_SIZE);
		org.apache.logging.log4j.core.Logger coreLogger = (org.apache.logging.log4j.core.Logger) logger;
		coreLogger.addAppender(obsHttpTMLog4jAppender);
		coreLogger.setAdditive(false);
		BurstFilter infoBurstFilter = BurstFilter.newBuilder()
				.setLevel(Level.INFO)
				.build();
		this.obsHttpTMLog4jAppender.addFilter(infoBurstFilter);
		this.obsHttpTMLog4jAppender.start();
	}

	@Override
	public void append(MonitoringLogsDto log) {
		if (null == log) {
			return;
		}
		if (!obsHttpTMLog4jAppender.isStarted()) {
			return;
		}
		String logJson;
		try {
			logJson = JSONUtil.obj2Json(log);
		} catch (JsonProcessingException e) {
			rootLogger.warn("Serialize monitor log to json failed, will ignored it\nEntity: {}\nError: {}", log, e.getMessage());
			return;
		}
		if (!obsHttpTMLog4jAppender.isStarted()) {
			return;
		}
		this.logger.info(logJson);
	}

	@Override
	public void append(List<MonitoringLogsDto> logs) {
		if (CollectionUtils.isEmpty(logs)) {
			return;
		}
		if (!obsHttpTMLog4jAppender.isStarted()) {
			return;
		}
		for (MonitoringLogsDto log : logs) {
			append(log);
		}
	}

	@Override
	public void stop() {
		if (null != logger) {
			removeAppenders((org.apache.logging.log4j.core.Logger) logger);
		}
	}
}
