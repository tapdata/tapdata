package io.tapdata.observable.alert;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Posts structured task alerts to TM through the existing engine HTTP operator.
 */
public class HttpTaskAlertPublisher implements TaskAlertPublisher {
    public static final String RESOURCE = ConnectorConstant.TASK_ALARM + "/task-alerts";

    private final ClientMongoOperator clientMongoOperator;

    public HttpTaskAlertPublisher() {
        this(BeanUtil.getBean(ClientMongoOperator.class));
    }

    public HttpTaskAlertPublisher(ClientMongoOperator clientMongoOperator) {
        this.clientMongoOperator = clientMongoOperator;
    }

    @Override
    public PublishResult publish(TaskAlertEvent event) {
        if (clientMongoOperator == null) {
            TaskAlertAudit.publishFailed(typeName(event), event.getCode(), "operator_unavailable", null);
            return PublishResult.RETRYABLE;
        }
        try {
            clientMongoOperator.insertOne(toRequestBody(event), RESOURCE);
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
