package io.tapdata.dql.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.constant.JSONUtil;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.dql.model.DqlEventReport;
import io.tapdata.dql.model.DqlEventReportResult;
import io.tapdata.dql.model.DqlRecordSuccessReport;
import io.tapdata.dql.model.DqlRecordSuccessReportResult;
import io.tapdata.dql.model.DqlRecoveryReport;
import io.tapdata.exception.ManagementException;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Objects;

/**
 * Small Engine-side adapter for the TM DQL internal callbacks.
 *
 * <p>The adapter deliberately delegates authentication, retry and TM error mapping to
 * {@link HttpClientMongoOperator}; it only owns the DQL resource paths and request models.</p>
 */
public class DqlTmClient {
    public static final String EVENT_REPORT_RESOURCE = "task/%s/dql-events/report";
    public static final String RECORD_SUCCESS_REPORT_RESOURCE = "task/%s/dql-events/record-success/report";
    public static final String RECOVERY_REPORT_RESOURCE = "task/%s/dql-events/recovery/report";

    private final HttpClientMongoOperator operator;

    public DqlTmClient(HttpClientMongoOperator operator) {
        this.operator = Objects.requireNonNull(operator, "operator must not be null");
    }

    public DqlEventReportResult reportEvent(String taskId, DqlEventReport report) {
        return post(taskId, report, EVENT_REPORT_RESOURCE, DqlEventReportResult.class);
    }

    public DqlRecordSuccessReportResult reportRecordSuccess(String taskId, DqlRecordSuccessReport report) {
        return post(taskId, report, RECORD_SUCCESS_REPORT_RESOURCE, DqlRecordSuccessReportResult.class);
    }

    public Boolean reportRecovery(String taskId, DqlRecoveryReport report) {
        return post(taskId, report, RECOVERY_REPORT_RESOURCE, Boolean.class);
    }

    private <T> T post(String taskId, Object request, String resourcePattern, Class<T> responseType) {
        if (StringUtils.isBlank(taskId)) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }
        Map<String, Object> payload = JSONUtil.mapper.convertValue(request,
                new TypeReference<Map<String, Object>>() {
                });
        T response = operator.postOne(payload, String.format(resourcePattern, taskId), responseType);
        if (response == null) {
            throw new ManagementException("DQL TM request returned an empty response");
        }
        return response;
    }
}
