package io.tapdata.entity;

import com.tapdata.cache.ICacheService;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.Connections;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.Job;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.ConverterProvider;
import io.tapdata.common.SettingService;
import io.tapdata.debug.DebugProcessor;
import io.tapdata.milestone.MilestoneService;
import org.apache.logging.log4j.Logger;

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

	public SourceContext(Job job,
						 Logger logger,
						 Object offset,
						 SettingService settingService,
						 Connections sourceConn,
						 Connections targetConn,
						 Consumer<List<MessageEntity>> messageConsumer,
						 String baseUrl,
						 String accessCode,
						 int restRetryTime,
						 String userId,
						 Integer roleId,
						 DebugProcessor debugProcessor,
						 List<JavaScriptFunctions> javaScriptFunctions,
						 ClientMongoOperator clientMongoOperator,
						 ICacheService cacheService,
						 ConverterProvider converterProvider,
						 MilestoneService milestoneService,
						 boolean isCloud,
						 ConfigurationCenter configurationCenter
	) {
		super(job, logger, offset, settingService, sourceConn, targetConn, debugProcessor, javaScriptFunctions, cacheService, converterProvider, milestoneService, configurationCenter);
		this.messageConsumer = messageConsumer;
		this.baseUrl = baseUrl;
		this.accessCode = accessCode;
		this.restRetryTime = restRetryTime;
		this.userId = userId;
		this.roleId = roleId;
		this.clientMongoOperator = clientMongoOperator;
		this.isCloud = isCloud;
	}

	public SourceContext(V1EngineContext context) {
		super(
				context.getJob(),
				context.getLogger(),
				context.getOffset(),
				context.getSettingService(),
				context.getSourceConn(),
				context.getTargetConn(),
				context.getDebugProcessor(),
				context.getJavaScriptFunctions(),
				context.getCacheService(),
				context.getConverterProvider(),
				context.getMilestoneService(),
				context.getDataFlow()
		);
		this.messageConsumer = context.getMessageConsumer();
		this.baseUrl = context.getBaseUrl();
		this.accessCode = context.getAccessCode();
		this.restRetryTime = context.getRestRetryTime();
		this.userId = context.getUserId();
		this.roleId = context.getRoleId();
		this.clientMongoOperator = context.getClientMongoOperator();
		this.isCloud = context.isCloud();
	}

	public SourceContext(V1EngineContext context,
						 TaskDto taskDto,
						 Node<?> node, ConfigurationCenter configurationCenter) {
		super(
				context.getJob(),
				context.getLogger(),
				context.getOffset(),
				context.getSettingService(),
				context.getSourceConn(),
				context.getTargetConn(),
				context.getDebugProcessor(),
				context.getJavaScriptFunctions(),
				context.getCacheService(),
				context.getConverterProvider(),
				context.getMilestoneService(),
				context.getDataFlow(),
				taskDto,
				node, configurationCenter
		);
		this.messageConsumer = context.getMessageConsumer();
		this.baseUrl = context.getBaseUrl();
		this.accessCode = context.getAccessCode();
		this.restRetryTime = context.getRestRetryTime();
		this.userId = context.getUserId();
		this.roleId = context.getRoleId();
		this.clientMongoOperator = context.getClientMongoOperator();
		this.isCloud = context.isCloud();
	}

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
}
