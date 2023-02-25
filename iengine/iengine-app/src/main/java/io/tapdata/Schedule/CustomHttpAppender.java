package io.tapdata.Schedule;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.observable.logging.LogEventData;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
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

import java.util.*;
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

		if (null != contextMap) {
			String taskId = contextMap.getValue("taskId");
			if (null != taskId) {
				ObsLogger obsLogger = ObsLoggerFactory.getInstance().getObsLogger(taskId);
				if (null != obsLogger) {
					// only keep pdk loggers
					if (null == message || !StringUtils.strip(message).startsWith("$$tag::")) {
						return;
					}
					switch (level.toUpperCase()) {
						case "INFO":
							obsLogger.info(message);
							break;
						case "WARN":
							obsLogger.warn(message);
							break;
						case "ERROR":
							Throwable throwable = event.getThrown();
							if (null == throwable) {
								obsLogger.error(message);
								break;
							}

							List<TapEvent> events = null;
							ProcessorBaseContext context = null;
							if (throwable instanceof NodeException) {
								NodeException nodeException = (NodeException) throwable;
								events = nodeException.getEvents();
								context = nodeException.getContext();
							}

							error(obsLogger, taskId, message, throwable, events, context);
							break;
						default:
					}
				}
			}
		}
	}

	private void error(ObsLogger obsLogger, String taskId, String message, Throwable throwable, List<TapEvent> events, ProcessorBaseContext context) {
		if (null == events || events.isEmpty()) {
			error(obsLogger, taskId, message, throwable, context);
			return;
		}

		Node<?> node = context.getNode();
		List<Map<String, Object>> data = new ArrayList<>();
		for(TapEvent event : events) {
			if (null == event) {
				continue;
			}
			TapBaseEvent baseEvent = (TapBaseEvent) event;
			Collection<String> pkFields = getPkFields(context, baseEvent.getTableId());
			data.add(LogEventData.builder()
					.eventType(LogEventData.LOG_EVENT_TYPE_PROCESS)
					.status(LogEventData.LOG_EVENT_STATUS_ERROR)
					.message(message)
					.time(System.currentTimeMillis())
					.withNode(node)
					.withTapEvent(event, pkFields)
					.build().toMap());
		}
		if (null != node) {
			ObsLogger nodeObsLogger = ObsLoggerFactory.getInstance().getObsLogger(taskId, node.getId());
			if (null != nodeObsLogger) {
				obsLogger = nodeObsLogger;
			}
		}

		ObsLogger obsLoggerFinal = obsLogger;
		obsLoggerFinal.error(() -> obsLoggerFinal.logBaseBuilder().data(data), throwable, message);
	}

	private void error(ObsLogger obsLogger, String taskId, String message, Throwable throwable, ProcessorBaseContext context) {
		if (null == context) {
			error(obsLogger, throwable);
			return;
		}

		Node<?> node = context.getNode();
		if (null == node) {
			error(obsLogger, throwable);
			return;
		}

		LogEventData.LogEventDataBuilder builder = LogEventData.builder()
				.eventType(LogEventData.LOG_EVENT_TYPE_PROCESS)
				.status(LogEventData.LOG_EVENT_STATUS_ERROR)
				.message(throwable.getMessage())
				.time(System.currentTimeMillis())
				.withNode(node);

		if (null != node) {
			ObsLogger nodeObsLogger = ObsLoggerFactory.getInstance().getObsLogger(taskId, node.getId());
			if (null != nodeObsLogger) {
				obsLogger = nodeObsLogger;
			}
		}

		ObsLogger obsLoggerFinal = obsLogger;
		obsLoggerFinal.error(() -> obsLoggerFinal.logBaseBuilder().record(builder.build().toMap()), throwable);
	}

	private void error(ObsLogger obsLogger, Throwable throwable) {
		obsLogger.error(obsLogger::logBaseBuilder, throwable);
	}

	private Collection<String> getPkFields(ProcessorBaseContext context, String tableName) {
		if (!(context instanceof DataProcessorContext)) {
			return Collections.emptyList();
		}

		TapTable table = context.getTapTableMap().get(tableName);
		if (null == table) {
			return Collections.emptyList();
		}

		Collection<String> pkFields =  table.primaryKeys();
		if (null == pkFields || pkFields.isEmpty()) {
			pkFields = table.primaryKeys(true);
		}

		if (null == pkFields || pkFields.isEmpty()) {
			pkFields = table.getNameFieldMap().keySet();
		}

		return pkFields;
	}
}
