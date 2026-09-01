package io.tapdata.dql.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.constant.JSONUtil;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.dql.vo.DqlRecoveryPayloadVo;
import io.tapdata.dql.model.DqlEventReport;
import io.tapdata.dql.model.DqlEventReportResult;
import io.tapdata.dql.model.DqlRecordSuccessReport;
import io.tapdata.dql.model.DqlRecordSuccessReportResult;
import io.tapdata.dql.model.DqlRecoveryReport;
import io.tapdata.dql.model.DqlStormGuardReport;
import io.tapdata.exception.ManagementException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Map;
import java.util.Objects;

/**
 * Small Engine-side adapter for the TM DLQ internal callbacks.
 *
 * <p>The adapter deliberately delegates authentication, retry and TM error mapping to
 * {@link HttpClientMongoOperator}; it only owns the DLQ resource paths and request models.</p>
 */
public class DqlTmClient {
    public static final String EVENT_REPORT_RESOURCE = "task/%s/dql-events/report";
    public static final String RECORD_SUCCESS_REPORT_RESOURCE = "task/%s/dql-events/record-success/report";
    public static final String RECOVERY_REPORT_RESOURCE = "task/%s/dql-events/recovery/report";
    public static final String STORM_GUARD_REPORT_RESOURCE = "task/%s/dql-events/storm-guard/report";
    public static final String RECOVERY_PAYLOAD_RESOURCE = "dql-events/%s/recovery-payload";

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

    public Boolean reportStormGuard(String taskId, DqlStormGuardReport report) {
        return post(taskId, report, STORM_GUARD_REPORT_RESOURCE, Boolean.class);
    }

    public DqlRecoveryPayloadVo getRecoveryPayload(String eventId) {
        if (StringUtils.isBlank(eventId)) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        DqlRecoveryPayloadVo payload = operator.findOne(
                new Query(), String.format(RECOVERY_PAYLOAD_RESOURCE, eventId), DqlRecoveryPayloadVo.class);
        if (payload == null) {
            throw new ManagementException("DLQ recovery payload was not found: " + eventId);
        }
        return payload;
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
            throw new ManagementException("DLQ TM request returned an empty response");
        }
        return response;
    }
}
