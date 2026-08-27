package io.tapdata.dql;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import io.tapdata.dql.client.DqlTmClient;
import io.tapdata.dql.model.DqlEventReport;
import io.tapdata.dql.model.DqlEventReportResult;
import io.tapdata.dql.model.DqlRecordSuccessReport;
import io.tapdata.dql.model.DqlRecordSuccessReportResult;
import io.tapdata.dql.model.DqlRecoveryReport;
import io.tapdata.dql.model.DqlRouteDecision;
import io.tapdata.dql.reporter.DqlEventReportException;
import io.tapdata.dql.reporter.DqlEventReporter;
import io.tapdata.exception.ManagementException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlEventReporterTest {
    private static final String TASK_ID = "task-1";

    @Test
    @DisplayName("reporter returns a normal TM acknowledgement")
    void reportsEvent() {
        DqlEventReportResult expected = result("DQL-1", "PENDING", false);
        RecordingDqlTmClient client = new RecordingDqlTmClient(expected);
        DqlEventReporter reporter = new DqlEventReporter(client);

        DqlEventReportResult actual = reporter.report(TASK_ID, report());

        assertSame(expected, actual);
        assertEquals(1, client.reportCalls);
    }

    @Test
    @DisplayName("reporter returns a later-success TM acknowledgement")
    void reportsRecordSuccess() {
        DqlRecordSuccessReportResult expected = new DqlRecordSuccessReportResult();
        expected.setMarked(true);
        expected.setEventId("DQL-1");
        RecordingDqlTmClient client = new RecordingDqlTmClient(expected);
        DqlEventReporter reporter = new DqlEventReporter(client);

        DqlRecordSuccessReport actualReport = new DqlRecordSuccessReport();
        actualReport.setTaskRecordId("record-1");
        DqlRecordSuccessReportResult actual = reporter.reportRecordSuccess(TASK_ID, actualReport);

        assertSame(expected, actual);
        assertEquals(1, client.successReportCalls);
    }

    @Test
    @DisplayName("recovery reporter requires a successful TM acknowledgement")
    void reportsRecovery() {
        RecordingDqlTmClient client = new RecordingDqlTmClient(Boolean.TRUE);
        DqlEventReporter reporter = new DqlEventReporter(client);

        Boolean acknowledged = reporter.reportRecovery(TASK_ID, DqlRecoveryReport.batchStarted("batch-1", 100L));

        assertTrue(acknowledged);
        assertEquals(1, client.recoveryReportCalls);
    }

    @Test
    @DisplayName("duplicate TM acknowledgement is a successful report")
    void duplicateResponseIsSuccess() {
        DqlEventReportResult expected = result("DQL-1", "PENDING", true);
        RecordingDqlTmClient client = new RecordingDqlTmClient(expected);
        DqlEventReporter reporter = new DqlEventReporter(client);

        DqlEventReportResult actual = reporter.report(TASK_ID, report());

        assertTrue(actual.isDuplicate());
        assertSame(expected, actual);
        assertEquals(1, client.reportCalls);
    }

    @Test
    @DisplayName("TM failure is escalated without exposing report payload")
    void tmFailureIsWrapped() {
        ManagementException cause = new ManagementException("TM unavailable; payload=secret-value");
        RecordingDqlTmClient client = new RecordingDqlTmClient(cause);
        DqlEventReporter reporter = new DqlEventReporter(client);

        DqlEventReportException exception = assertThrows(DqlEventReportException.class,
                () -> reporter.report(TASK_ID, report()));

        assertSame(cause, exception.getCause());
        assertEquals(TASK_ID, exception.getTaskId());
        assertTrue(exception.getMessage().contains(TASK_ID));
        assertFalse(exception.getMessage().contains("secret-value"));
        assertEquals(1, client.reportCalls);
    }

    @Test
    @DisplayName("incomplete TM acknowledgement is treated as a report failure")
    void incompleteResponseFails() {
        RecordingDqlTmClient client = new RecordingDqlTmClient(result(" ", "PENDING", false));
        DqlEventReporter reporter = new DqlEventReporter(client);

        DqlEventReportException exception = assertThrows(DqlEventReportException.class,
                () -> reporter.report(TASK_ID, report()));

        assertEquals(TASK_ID, exception.getTaskId());
        assertEquals(1, client.reportCalls);
    }

    @Test
    @DisplayName("invalid report route is rejected before calling TM")
    void invalidRouteIsRejectedBeforeCallingTm() {
        RecordingDqlTmClient client = new RecordingDqlTmClient(result("DQL-1", "PENDING", false));
        DqlEventReporter reporter = new DqlEventReporter(client);
        DqlEventReport invalidReport = report();
        invalidReport.setRouteDecision(DqlRouteDecision.TASK_RETRY);

        assertThrows(IllegalArgumentException.class, () -> reporter.report(TASK_ID, invalidReport));
        assertEquals(0, client.reportCalls);
    }

    @Test
    @DisplayName("invalid report input is rejected before calling TM")
    void invalidInputIsRejectedBeforeCallingTm() {
        RecordingDqlTmClient client = new RecordingDqlTmClient(result("DQL-1", "PENDING", false));
        DqlEventReporter reporter = new DqlEventReporter(client);

        assertThrows(IllegalArgumentException.class, () -> reporter.report(" ", report()));
        assertThrows(IllegalArgumentException.class, () -> reporter.report(TASK_ID, null));
        assertEquals(0, client.reportCalls);
    }

    private static DqlEventReport report() {
        DqlEventReport report = new DqlEventReport();
        report.setTaskRecordId("record-1");
        report.setRouteDecision(DqlRouteDecision.RECORD_DLQ);
        return report;
    }

    private static DqlEventReportResult result(String eventId, String status, boolean duplicate) {
        DqlEventReportResult result = new DqlEventReportResult();
        result.setEventId(eventId);
        result.setStatus(status);
        result.setDuplicate(duplicate);
        return result;
    }

    private static class RecordingDqlTmClient extends DqlTmClient {
        private final DqlEventReportResult response;
        private final DqlRecordSuccessReportResult successResponse;
        private final Boolean recoveryResponse;
        private final RuntimeException failure;
        private int reportCalls;
        private int successReportCalls;
        private int recoveryReportCalls;

        private RecordingDqlTmClient(DqlEventReportResult response) {
            super(new NoopMongoOperator());
            this.response = response;
            this.successResponse = null;
            this.recoveryResponse = null;
            this.failure = null;
        }

        private RecordingDqlTmClient(DqlRecordSuccessReportResult response) {
            super(new NoopMongoOperator());
            this.response = null;
            this.successResponse = response;
            this.recoveryResponse = null;
            this.failure = null;
        }

        private RecordingDqlTmClient(Boolean response) {
            super(new NoopMongoOperator());
            this.response = null;
            this.successResponse = null;
            this.recoveryResponse = response;
            this.failure = null;
        }

        private RecordingDqlTmClient(RuntimeException failure) {
            super(new NoopMongoOperator());
            this.response = null;
            this.successResponse = null;
            this.recoveryResponse = null;
            this.failure = failure;
        }

        @Override
        public DqlEventReportResult reportEvent(String taskId, DqlEventReport report) {
            reportCalls++;
            if (failure != null) {
                throw failure;
            }
            return response;
        }

        @Override
        public DqlRecordSuccessReportResult reportRecordSuccess(String taskId, DqlRecordSuccessReport report) {
            successReportCalls++;
            if (failure != null) {
                throw failure;
            }
            return successResponse;
        }

        @Override
        public Boolean reportRecovery(String taskId, DqlRecoveryReport report) {
            recoveryReportCalls++;
            if (failure != null) {
                throw failure;
            }
            return recoveryResponse;
        }
    }

    private static class NoopMongoOperator extends HttpClientMongoOperator {
        private NoopMongoOperator() {
            super(null, null, new RestTemplateOperator(Collections.singletonList("http://localhost"), 0),
                    new ConfigurationCenter());
        }
    }
}
