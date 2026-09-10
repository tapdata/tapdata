package io.tapdata.observable.alert;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Posts structured task alerts to TM through the existing engine HTTP operator.
 *
 * The JVM dispatcher singleton can be created during {@code SpringApplication.run()},
 * before {@link BeanUtil#configurableApplicationContext} is assigned. Resolve the
 * operator lazily so a boot-time null does not permanently disable publishing.
 */
public class HttpTaskAlertPublisher implements TaskAlertPublisher {
    public static final String RESOURCE = ConnectorConstant.TASK_ALARM + "/task-alerts";
    static final String CLIENT_MONGO_OPERATOR_BEAN = "clientMongoOperator";

    private volatile ClientMongoOperator clientMongoOperator;
    private final boolean operatorInjected;

    public HttpTaskAlertPublisher() {
        this.operatorInjected = false;
    }

    public HttpTaskAlertPublisher(ClientMongoOperator clientMongoOperator) {
        this.clientMongoOperator = clientMongoOperator;
        this.operatorInjected = true;
    }

    @Override
    public PublishResult publish(TaskAlertEvent event) {
        ClientMongoOperator operator = resolveOperator();
        if (operator == null) {
            TaskAlertAudit.publishFailed(typeName(event), event.getCode(), "operator_unavailable", null);
            return PublishResult.RETRYABLE;
        }
        try {
            operator.insertOne(toRequestBody(event), RESOURCE);
            return PublishResult.SUCCESS;
        } catch (RuntimeException runtimeException) {
            HttpClientErrorException clientErrorException = findHttpClientError(runtimeException);
            if (clientErrorException != null) {
                int status = clientErrorException.getStatusCode().value();
                if (status == 409) {
                    return PublishResult.SUCCESS;
                }
                if (status == 429) {
                    return PublishResult.RETRYABLE;
                }
                if (status >= 400 && status < 500) {
                    TaskAlertAudit.tmRejected(typeName(event), event.getCode(), "http_" + status);
                    return PublishResult.NON_RETRYABLE;
                }
                return PublishResult.RETRYABLE;
            }
            if (runtimeException instanceof HttpServerErrorException) {
                return PublishResult.RETRYABLE;
            }
            if (isNonRetryable(runtimeException)) {
                TaskAlertAudit.tmRejected(typeName(event), event.getCode(), runtimeException.getClass().getSimpleName());
                return PublishResult.NON_RETRYABLE;
            }
            return PublishResult.RETRYABLE;
        }
    }

    ClientMongoOperator resolveOperator() {
        if (operatorInjected) {
            return this.clientMongoOperator;
        }
        ClientMongoOperator current = this.clientMongoOperator;
        if (current != null) {
            return current;
        }
        ClientMongoOperator resolved = lookupOperator();
        if (resolved != null) {
            this.clientMongoOperator = resolved;
        }
        return resolved;
    }

    private static ClientMongoOperator lookupOperator() {
        try {
            ConfigurableApplicationContext context = BeanUtil.configurableApplicationContext;
            if (context != null) {
                if (context.containsBean(CLIENT_MONGO_OPERATOR_BEAN)) {
                    return context.getBean(CLIENT_MONGO_OPERATOR_BEAN, ClientMongoOperator.class);
                }
                return context.getBean(ClientMongoOperator.class);
            }
            return BeanUtil.getBean(ClientMongoOperator.class);
        } catch (RuntimeException runtimeException) {
            TaskAlertAudit.publishFailed(null, null, "operator_lookup", runtimeException);
            return null;
        }
    }

    private static HttpClientErrorException findHttpClientError(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof HttpClientErrorException) {
                return (HttpClientErrorException) current;
            }
            current = current.getCause();
        }
        return null;
    }

    protected Map<String, Object> toRequestBody(TaskAlertEvent event) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("taskId", event.getTaskId());
        body.put("nodeId", event.getNodeId());
        body.put("nodeName", event.getNodeName());
        body.put("message", event.getMessage());
        return body;
    }

    private static boolean isNonRetryable(Throwable throwable) {
        String message = throwable.getMessage();
        return message != null && (message.contains("400") || message.contains("401")
                || message.contains("403") || message.contains("404") || message.contains("422"));
    }

    private static String typeName(TaskAlertEvent event) {
        return event.getType() == null ? null : event.getType().name();
    }
}
