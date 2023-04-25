package io.tapdata.flow.engine.manager.log;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
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

//        try {
		byte[] bytes = layout.toByteArray(event);
//            System.out.println(new String(bytes));
		Document entity = new Document();
		entity.put("level", event.getLevel().name());
		entity.put("loggerName", event.getLoggerName());
		entity.put("message", event.getMessage() == null ? null : event.getMessage().getFormattedMessage());

//        final StackTraceElement source = event.getSource();
//        if (source == null) {
//            entity.set("source", (Object) null);
//        } else {
//            entity.set("source", this.convertStackTraceElement(source));
//        }

//        final Marker marker = event.getMarker();
//        if (marker == null) {
//            entity.set("marker", (Object) null);
//        } else {
//            entity.set("marker", buildMarkerEntity(marker));
//        }

		entity.put("threadId", event.getThreadId());
		entity.put("threadName", event.getThreadName());
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

//            exceptionEntity.set("stackTrace", this.convertStackTrace(thrown.getStackTrace()));
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
//                causingExceptionEntity.set("stackTrace", this.convertStackTrace(thrown.getStackTrace()));
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

//            String byteToStr = new String(bytes);
//            String s = JSONUtil.obj2Json(event);
//            System.out.println(byteToStr);
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
//        }

		clientMongoOperator.insertOne(entity, ConnectorConstant.LOG_COLLECTION);
//        eventMap.put(Instant.now().toString(), event);
	}
}
