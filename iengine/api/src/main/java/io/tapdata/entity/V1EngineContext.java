package io.tapdata.entity;

import com.tapdata.cache.ICacheService;
import com.tapdata.entity.Connections;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.Job;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.ConverterProvider;
import io.tapdata.common.SettingService;
import io.tapdata.debug.DebugProcessor;
import io.tapdata.milestone.MilestoneService;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.function.Consumer;

/**
 * 老版引擎上下文信息
 *
 * @author jackin
 * @date 2021/7/14 3:44 PM
 **/
public class V1EngineContext {

	private Consumer<List<MessageEntity>> messageConsumer;
	private String baseUrl;
	private String accessCode;
	private int restRetryTime;
	private String userId;
	private Integer roleId;
	private ClientMongoOperator clientMongoOperator;
	private boolean isCloud;
	private Job job;
	private Logger logger;

	private Object offset;

	private SettingService settingService;

	private Connections sourceConn;
	private Connections targetConn;

	private DebugProcessor debugProcessor;

	private List<JavaScriptFunctions> javaScriptFunctions;

	/**
	 * cache 数据
	 */
	private ICacheService cacheService;

	private ConverterProvider converterProvider;

	private MilestoneService milestoneService;

	private DataFlow dataFlow;

	private V1EngineContext() {
	}

	public static final class Builder {
		private Consumer<List<MessageEntity>> messageConsumer;
		private String baseUrl;
		private String accessCode;
		private int restRetryTime;
		private String userId;
		private Integer roleId;
		private ClientMongoOperator clientMongoOperator;
		private boolean isCloud;
		private Job job;
		private Logger logger;
		private Object offset;
		private SettingService settingService;
		private Connections sourceConn;
		private Connections targetConn;
		private DebugProcessor debugProcessor;
		private List<JavaScriptFunctions> javaScriptFunctions;
		private ConverterProvider converterProvider;
		private MilestoneService milestoneService;
		private DataFlow dataFlow;
		private ICacheService cacheService;

		public Builder() {
		}

		public Builder withMessageConsumer(Consumer<List<MessageEntity>> messageConsumer) {
			this.messageConsumer = messageConsumer;
			return this;
		}

		public Builder withBaseUrl(String baseUrl) {
			this.baseUrl = baseUrl;
			return this;
		}

		public Builder withAccessCode(String accessCode) {
			this.accessCode = accessCode;
			return this;
		}

		public Builder withRestRetryTime(int restRetryTime) {
			this.restRetryTime = restRetryTime;
			return this;
		}

		public Builder withUserId(String userId) {
			this.userId = userId;
			return this;
		}

		public Builder withRoleId(Integer roleId) {
			this.roleId = roleId;
			return this;
		}

		public Builder withClientMongoOperator(ClientMongoOperator clientMongoOperator) {
			this.clientMongoOperator = clientMongoOperator;
			return this;
		}

		public Builder withIsCloud(boolean isCloud) {
			this.isCloud = isCloud;
			return this;
		}

		public Builder withJob(Job job) {
			this.job = job;
			return this;
		}

		public Builder withLogger(Logger logger) {
			this.logger = logger;
			return this;
		}

		public Builder withOffset(Object offset) {
			this.offset = offset;
			return this;
		}

		public Builder withSettingService(SettingService settingService) {
			this.settingService = settingService;
			return this;
		}

		public Builder withSourceConn(Connections sourceConn) {
			this.sourceConn = sourceConn;
			return this;
		}

		public Builder withTargetConn(Connections targetConn) {
			this.targetConn = targetConn;
			return this;
		}

		public Builder withDebugProcessor(DebugProcessor debugProcessor) {
			this.debugProcessor = debugProcessor;
			return this;
		}

		public Builder withJavaScriptFunctions(List<JavaScriptFunctions> javaScriptFunctions) {
			this.javaScriptFunctions = javaScriptFunctions;
			return this;
		}

		public Builder withConverterProvider(ConverterProvider converterProvider) {
			this.converterProvider = converterProvider;
			return this;
		}

		public Builder withMilestoneService(MilestoneService milestoneService) {
			this.milestoneService = milestoneService;
			return this;
		}

		public Builder withDataFlow(DataFlow dataFlow) {
			this.dataFlow = dataFlow;
			return this;
		}

		public Builder withCacheService(ICacheService cacheService) {
			this.cacheService = cacheService;
			return this;
		}


		public V1EngineContext build() {
			V1EngineContext v1EngineContext = new V1EngineContext();
			v1EngineContext.cacheService = this.cacheService;
			v1EngineContext.logger = this.logger;
			v1EngineContext.userId = this.userId;
			v1EngineContext.converterProvider = this.converterProvider;
			v1EngineContext.debugProcessor = this.debugProcessor;
			v1EngineContext.milestoneService = this.milestoneService;
			v1EngineContext.javaScriptFunctions = this.javaScriptFunctions;
			v1EngineContext.clientMongoOperator = this.clientMongoOperator;
			v1EngineContext.targetConn = this.targetConn;
			v1EngineContext.settingService = this.settingService;
			v1EngineContext.messageConsumer = this.messageConsumer;
			v1EngineContext.roleId = this.roleId;
			v1EngineContext.restRetryTime = this.restRetryTime;
			v1EngineContext.sourceConn = this.sourceConn;
			v1EngineContext.baseUrl = this.baseUrl;
			v1EngineContext.isCloud = this.isCloud;
			v1EngineContext.job = this.job;
			v1EngineContext.offset = this.offset;
			v1EngineContext.accessCode = this.accessCode;
			v1EngineContext.dataFlow = this.dataFlow;
			return v1EngineContext;
		}
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

	public boolean isCloud() {
		return isCloud;
	}

	public Job getJob() {
		return job;
	}

	public Logger getLogger() {
		return logger;
	}

	public Object getOffset() {
		return offset;
	}

	public SettingService getSettingService() {
		return settingService;
	}

	public Connections getSourceConn() {
		return sourceConn;
	}

	public Connections getTargetConn() {
		return targetConn;
	}

	public DebugProcessor getDebugProcessor() {
		return debugProcessor;
	}

	public List<JavaScriptFunctions> getJavaScriptFunctions() {
		return javaScriptFunctions;
	}

	public ICacheService getCacheService() {
		return cacheService;
	}

	public ConverterProvider getConverterProvider() {
		return converterProvider;
	}

	public MilestoneService getMilestoneService() {
		return milestoneService;
	}

	public DataFlow getDataFlow() {
		return dataFlow;
	}
}
