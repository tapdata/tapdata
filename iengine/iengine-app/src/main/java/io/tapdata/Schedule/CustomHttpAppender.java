package io.tapdata.Schedule;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.logging.JobCustomerLogger;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.util.ReadOnlyStringMap;
import org.bson.Document;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Plugin(
		name = "CustomHttpAppender",
		category = Core.CATEGORY_NAME,
		elementType = Appender.ELEMENT_TYPE)
public class CustomHttpAppender extends AbstractAppender {

	private ConcurrentMap<String, LogEvent> eventMap = new ConcurrentHashMap<>();

	private Layout layout;

	private ClientMongoOperator clientMongoOperator;

	protected CustomHttpAppender(String name, Filter filter, Layout layout, ClientMongoOperator clientMongoOperator) {
		super(name, filter, layout);
		this.layout = layout;
		this.clientMongoOperator = clientMongoOperator;
	}

	@PluginFactory
	public static CustomHttpAppender createAppender(
			@PluginAttribute("name") String name,
			@PluginElement("Filter") Filter filter, Layout layout, ClientMongoOperator clientMongoOperator) {

		return new CustomHttpAppender(name, filter, layout, clientMongoOperator);
	}

	@Override
	public void append(LogEvent event) {

		Document entity = new Document();
		String level = event.getLevel().name();
		entity.put("level", level);
		entity.put("loggerName", event.getLoggerName());
		String message = event.getMessage() == null ? null : event.getMessage().getFormattedMessage();
		boolean knownCustomerErrLog = message != null && message.contains(JobCustomerLogger.CUSTOMER_ERROR_LOG_PREFIX);
		if (knownCustomerErrLog) {
			message = StringUtils.removeStart(message, JobCustomerLogger.CUSTOMER_ERROR_LOG_PREFIX);
		}
		entity.put("message", message);


		entity.put("threadId", event.getThreadId());
		entity.put("threadName", event.getThreadName());
		if (event.getContextData() != null
				&& event.getContextData().containsKey("threadName")
				&& StringUtils.isNotBlank(event.getContextData().getValue("threadName"))) {
			entity.put("threadName", event.getContextData().getValue("threadName"));
		}
		entity.put("threadPriority", event.getThreadPriority());
		entity.put("millis", event.getTimeMillis());
		entity.put("date", new java.util.Date(event.getTimeMillis()));

		@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
		Throwable thrown = event.getThrown();
		if (thrown == null) {
			entity.put("thrown", (Object) null);
		} else {
			final Document originalExceptionEntity = new Document();
			Document exceptionEntity = originalExceptionEntity;

			StringBuilder thrownMsg = new StringBuilder();
			thrownMsg.append(thrown.getMessage()).append('\n');
			for (StackTraceElement stackTraceElement : thrown.getStackTrace()) {
				thrownMsg.append(stackTraceElement.toString()).append('\n');
			}
			exceptionEntity.put("type", thrown.getClass().getName());
			exceptionEntity.put("message", thrownMsg.toString());
			while (thrown.getCause() != null) {
				thrown = thrown.getCause();
				final Document causingExceptionEntity = new Document();

				thrownMsg.setLength(0);
				thrownMsg.append(thrown.getMessage()).append('\n');
				for (StackTraceElement stackTraceElement : thrown.getStackTrace()) {
					thrownMsg.append(stackTraceElement.toString()).append('\n');
				}
				causingExceptionEntity.put("type", thrown.getClass().getName());
				causingExceptionEntity.put("message", thrownMsg.toString());
				exceptionEntity.put("cause", causingExceptionEntity);
				exceptionEntity = causingExceptionEntity;
			}

			entity.put("thrown", exceptionEntity);
		}

		final ReadOnlyStringMap contextMap = event.getContextData();
		if (contextMap == null) {
			entity.put("contextMap", (Object) null);
		} else {
			final Document contextMapEntity = new Document();
			contextMap.forEach((key, val) -> {
				contextMapEntity.put(key, val);
			});
			entity.put("contextMap", contextMapEntity);
		}

		final ThreadContext.ContextStack contextStack = event.getContextStack();
		if (contextStack == null) {
			entity.put("contextStack", (Object) null);
		} else {
			entity.put("contextStack", contextStack.asList().toArray());
		}

		clientMongoOperator.insertOne(entity, ConnectorConstant.LOG_COLLECTION);

		// add customer unknown error logs
		String jobName = ThreadContext.get("jobName");
		String dataFlowId = ThreadContext.get("dataFlowId");
		if (filterUnnecessaryUnknownErrorForCustomerErrorLog(knownCustomerErrLog, level, event.getLoggerName(), message)) {
			JobCustomerLogger.unknownError(dataFlowId, jobName, clientMongoOperator, message);
		}
	}


	/**
	 * Filter the unknown error logs, some logs are redundant，filter the logs out of the customer logs;
	 */
	private boolean filterUnnecessaryUnknownErrorForCustomerErrorLog(boolean knownCustomerErrLog, String level, String loggerName,
																	 String message) {
		if (!"ERROR".equals(level)) {
			return false;
		}
		if (knownCustomerErrLog) {
			return false;
		}
		// 1. filter the "To see additional job metrics enable JobConfig.storeMetricsAfterJobCompletion" error logged by jet
		if ("com.hazelcast.jet.impl.MasterJobContext".equals(loggerName)
				&& message.contains("To see additional job metrics enable JobConfig.storeMetricsAfterJobCompletion")) {
			return false;
		}

		return true;
	}
}
