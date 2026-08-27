package io.tapdata.dql.reporter;

import io.tapdata.dql.client.DqlTmClient;
import io.tapdata.dql.model.DqlEventReport;
import io.tapdata.dql.model.DqlEventReportResult;
import io.tapdata.dql.model.DqlRecordSuccessReport;
import io.tapdata.dql.model.DqlRecordSuccessReportResult;
import io.tapdata.dql.model.DqlRecoveryReport;
import io.tapdata.dql.model.DqlRouteDecision;
import io.tapdata.dql.model.DqlStormGuardReport;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * Orchestrates the Engine-side DQL event report and its TM acknowledgement.
 *
 * <p>HTTP timeout and transport retry remain owned by {@link DqlTmClient}'s
 * underlying TM client. This class has one orchestration call: a response is
 * successful only when TM returns an acknowledgement with an event id. A
 * duplicate acknowledgement is still a successful idempotent result.</p>
 */
public class DqlEventReporter {
    private final DqlTmClient tmClient;

    public DqlEventReporter(DqlTmClient tmClient) {
        this.tmClient = Objects.requireNonNull(tmClient, "tmClient must not be null");
    }

    public DqlEventReportResult report(String taskId, DqlEventReport report) {
        validateInput(taskId, report);
        try {
            DqlEventReportResult result = tmClient.reportEvent(taskId, report);
            validateAcknowledgement(taskId, result);
            return result;
        } catch (DqlEventReportException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new DqlEventReportException(taskId, exception);
        }
    }

    /**
     * Reports a successfully written record so TM can mark a matching
     * unresolved DQL event as exposed to a later write.
     */
    public DqlRecordSuccessReportResult reportRecordSuccess(String taskId, DqlRecordSuccessReport report) {
        validateRecordSuccessInput(taskId, report);
        try {
            DqlRecordSuccessReportResult result = tmClient.reportRecordSuccess(taskId, report);
            if (result == null) {
                throw new DqlEventReportException(taskId, "TM acknowledgement was empty");
            }
            return result;
        } catch (DqlEventReportException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new DqlEventReportException(taskId, exception);
        }
    }

    /**
     * Reports recovery lifecycle progress to TM. Recovery callbacks have a
     * separate state machine from capture reports, so they use a dedicated
     * request model and acknowledgement validation.
     */
    public Boolean reportRecovery(String taskId, DqlRecoveryReport report) {
        if (StringUtils.isBlank(taskId)) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        if (report == null) {
            throw new IllegalArgumentException("report must not be null");
        }
        try {
            Boolean acknowledged = tmClient.reportRecovery(taskId, report);
            if (!Boolean.TRUE.equals(acknowledged)) {
                throw new DqlEventReportException(taskId, "TM recovery acknowledgement was not successful");
            }
            return acknowledged;
        } catch (DqlEventReportException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new DqlEventReportException(taskId, exception);
        }
    }

    /**
     * Reports Storm Guard activation without affecting the task-level route if TM is unavailable.
     */
    public Boolean reportStormGuard(String taskId, DqlStormGuardReport report) {
        if (StringUtils.isBlank(taskId)) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        if (report == null) {
            throw new IllegalArgumentException("report must not be null");
        }
        try {
            Boolean acknowledged = tmClient.reportStormGuard(taskId, report);
            if (!Boolean.TRUE.equals(acknowledged)) {
                throw new DqlEventReportException(taskId, "TM Storm Guard acknowledgement was not successful");
            }
            return acknowledged;
        } catch (DqlEventReportException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new DqlEventReportException(taskId, exception);
        }
    }

    private void validateInput(String taskId, DqlEventReport report) {
        if (StringUtils.isBlank(taskId)) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        if (report == null) {
            throw new IllegalArgumentException("report must not be null");
        }
        if (report.getRouteDecision() != DqlRouteDecision.RECORD_DLQ) {
            throw new IllegalArgumentException("report routeDecision must be RECORD_DLQ");
        }
    }

    private void validateRecordSuccessInput(String taskId, DqlRecordSuccessReport report) {
        if (StringUtils.isBlank(taskId)) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        if (report == null) {
            throw new IllegalArgumentException("report must not be null");
        }
    }

    private void validateAcknowledgement(String taskId, DqlEventReportResult result) {
        if (result == null) {
            throw new DqlEventReportException(taskId, "TM acknowledgement was empty");
        }
        if (StringUtils.isBlank(result.getEventId())) {
            throw new DqlEventReportException(taskId, "TM acknowledgement had no event id");
        }
    }
}
