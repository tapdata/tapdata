package io.tapdata.entity;

import com.tapdata.cache.ICacheService;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.Job;
import com.tapdata.entity.TypeMapping;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.ConverterProvider;
import io.tapdata.common.SettingService;
import io.tapdata.debug.DebugProcessor;
import io.tapdata.milestone.MilestoneService;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import java.util.List;

public abstract class Context {

	protected Job job;

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

	private List<TypeMapping> targetTypeMappings;

	private ConfigurationCenter configurationCenter;

	private TaskDto taskDto;

	private Node<?> node;

	public Context(Job job, Logger logger, Object offset, SettingService settingService,
				   Connections sourceConn, Connections targetConn, DebugProcessor debugProcessor,
				   List<JavaScriptFunctions> javaScriptFunctions, ICacheService cacheService,
				   ConverterProvider converterProvider, MilestoneService milestoneService,
				   ConfigurationCenter configurationCenter) {
		this.job = job;
		this.logger = logger;
		this.offset = offset;
		this.settingService = settingService;
		this.sourceConn = sourceConn;
		this.targetConn = targetConn;
		this.debugProcessor = debugProcessor;
		this.javaScriptFunctions = javaScriptFunctions;
		this.cacheService = cacheService;
		this.converterProvider = converterProvider;
		this.milestoneService = milestoneService;
		this.configurationCenter = configurationCenter;
	}

	public Context(Job job, Logger logger, Object offset, SettingService settingService,
				   Connections sourceConn, Connections targetConn, DebugProcessor debugProcessor,
				   List<JavaScriptFunctions> javaScriptFunctions, ICacheService cacheService,
				   ConverterProvider converterProvider, MilestoneService milestoneService, DataFlow dataFlow) {
		this.job = job;
		this.logger = logger;
		this.offset = offset;
		this.settingService = settingService;
		this.sourceConn = sourceConn;
		this.targetConn = targetConn;
		this.debugProcessor = debugProcessor;
		this.javaScriptFunctions = javaScriptFunctions;
		this.cacheService = cacheService;
		this.converterProvider = converterProvider;
		this.milestoneService = milestoneService;
		this.dataFlow = dataFlow;
	}

	public Context(Job job, Logger logger, Object offset, SettingService settingService,
				   Connections sourceConn, Connections targetConn, DebugProcessor debugProcessor,
				   List<JavaScriptFunctions> javaScriptFunctions, ICacheService cacheService,
				   ConverterProvider converterProvider, MilestoneService milestoneService, DataFlow dataFlow,
				   TaskDto taskDto, Node<?> node, ConfigurationCenter configurationCenter) {
		this.job = job;
		this.logger = logger;
		this.offset = offset;
		this.settingService = settingService;
		this.sourceConn = sourceConn;
		this.targetConn = targetConn;
		this.debugProcessor = debugProcessor;
		this.javaScriptFunctions = javaScriptFunctions;
		this.cacheService = cacheService;
		this.converterProvider = converterProvider;
		this.milestoneService = milestoneService;
		this.dataFlow = dataFlow;
		this.taskDto = taskDto;
		this.node = node;
		this.configurationCenter = configurationCenter;
	}

	public Context(List<Stage> stages, Connections connection) {

	}

	public Connections getSourceConn() {
		return sourceConn;
	}

	public Connections getTargetConn() {
		return targetConn;
	}

	public Logger getLogger() {
		return logger;
	}

	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	public Job getJob() {
		return job;
	}

	public Object getOffset() {
		return offset;
	}

	public SettingService getSettingService() {
		return settingService;
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

	public void setConverterProvider(ConverterProvider converterProvider) {
		this.converterProvider = converterProvider;
	}

	public MilestoneService getMilestoneService() {
		return milestoneService;
	}

	public List<TypeMapping> getTargetTypeMappings() {
		return targetTypeMappings;
	}

	public void setTargetTypeMappings(List<TypeMapping> targetTypeMappings) {
		this.targetTypeMappings = targetTypeMappings;
	}

	public boolean isRunning() {
		return (StringUtils.equalsAny(getJob().getStatus(), ConnectorConstant.RUNNING)
				&& !Thread.currentThread().isInterrupted()) ||
				(dataFlow != null && StringUtils.equalsAny(dataFlow.getStatus(), ConnectorConstant.RUNNING));
	}

	public ConfigurationCenter getConfigurationCenter() {
		return configurationCenter;
	}

	public TaskDto getTaskDto() {
		return taskDto;
	}

	public Node<?> getNode() {
		return node;
	}
}
