package io.tapdata.entity;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.Connections;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.logging.JobCustomerLogger;

import java.util.List;
import java.util.function.Consumer;

public class SourceContext extends Context {

	private Consumer<List<MessageEntity>> messageConsumer;
	private String baseUrl;
	private String accessCode;
	private int restRetryTime;
	private String userId;
	private Integer roleId;
	private ClientMongoOperator clientMongoOperator;
	private boolean isCloud;

	private JobCustomerLogger customerLogger;

	public SourceContext(List<Stage> stages, Connections connection) {
		super(stages, connection);
	}

	public Consumer<List<MessageEntity>> getMessageConsumer() {
		return messageConsumer;
	}

	public String getBaseUrl() {
		return baseUrl;
	}

	public String getAccessCode() {
		return accessCode;
	}

	public int getRestRetryTime() {
		return restRetryTime;
	}

	public String getUserId() {
		return userId;
	}

	public Integer getRoleId() {
		return roleId;
	}

	public ClientMongoOperator getClientMongoOperator() {
		return clientMongoOperator;
	}

	public boolean getIsCloud() {
		return this.isCloud;
	}

	public void setIsCloud(boolean isCloud) {
		this.isCloud = isCloud;
	}

	public void setMessageConsumer(Consumer<List<MessageEntity>> messageConsumer) {
		this.messageConsumer = messageConsumer;
	}

	public JobCustomerLogger getCustomerLogger() {
		if (customerLogger == null) {
			customerLogger = new JobCustomerLogger();
		}
		return customerLogger;
	}
}
