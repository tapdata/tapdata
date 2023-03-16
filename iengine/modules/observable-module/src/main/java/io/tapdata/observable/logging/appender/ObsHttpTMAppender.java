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

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author samuel
 * @Description
 * @create 2023-03-08 15:40
 **/
public class ObsHttpTMAppender implements Appender<MonitoringLogsDto>, Serializable {

	private static final long serialVersionUID = -1887785881704714112L;
	private final Logger rootLogger = LogManager.getLogger(ObsHttpTMAppender.class);
	private Logger logger;
	private ObsHttpTMLog4jAppender obsHttpTMLog4jAppender;
	private final ClientMongoOperator clientMongoOperator;

	private ObsHttpTMAppender(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	public static ObsHttpTMAppender create(ClientMongoOperator clientMongoOperator) {
		return new ObsHttpTMAppender(clientMongoOperator);
	}

	public void start(String taskId) {
		this.obsHttpTMLog4jAppender = ObsHttpTMLog4jAppender.createAppender(String.join("-", ObsHttpTMLog4jAppender.class.getSimpleName(), taskId),
				null, null, false, null, clientMongoOperator, AppenderFactory.BATCH_SIZE);
		this.logger = LogManager.getLogger(String.join("-", ObsHttpTMAppender.class.getSimpleName(), taskId));
		((org.apache.logging.log4j.core.Logger) logger).addAppender(obsHttpTMLog4jAppender);
		((org.apache.logging.log4j.core.Logger) logger).setAdditive(false);
		BurstFilter infoBurstFilter = BurstFilter.newBuilder()
				.setLevel(Level.INFO)
				.build();
		this.obsHttpTMLog4jAppender.addFilter(infoBurstFilter);
		this.obsHttpTMLog4jAppender.start();
	}

	public void stop() {
		if (null != this.obsHttpTMLog4jAppender) {
			this.obsHttpTMLog4jAppender.stop(1L, TimeUnit.MINUTES);
		}
	}

	@Override
	public void append(List<MonitoringLogsDto> logs) {
		if (CollectionUtils.isEmpty(logs)) {
			return;
		}
		for (MonitoringLogsDto log : logs) {
			String logJson;
			try {
				logJson = JSONUtil.obj2Json(log);
			} catch (JsonProcessingException e) {
				rootLogger.warn("Serialize monitor log to json failed, will ignored it\nEntity: {}\nError: {}", log, e.getMessage());
				continue;
			}
			this.logger.info(logJson);
		}
	}
}
