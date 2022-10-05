package io.tapdata.observable.logging.appender;

import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/**
 * @author jackin
 * @date 2022/6/21 18:08
 **/
public class TMAppender implements Appender<MonitoringLogsDto> {

	private final ClientMongoOperator clientMongoOperator;

	public TMAppender(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	@Override
	public void append(List<MonitoringLogsDto> logs) {
		if (CollectionUtils.isNotEmpty(logs)) {
			this.clientMongoOperator.insertMany(logs, "MonitoringLogs/batch");
		}
	}
}
