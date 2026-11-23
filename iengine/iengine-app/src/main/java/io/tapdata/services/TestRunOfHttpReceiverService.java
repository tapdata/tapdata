package io.tapdata.services;

import com.tapdata.entity.Connections;
import io.tapdata.flow.engine.V2.log.CollectLog;
import io.tapdata.service.skeleton.annotation.RemoteService;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * @author GavinXiao
 * @description TestRunOfHttpReceiverService create by Gavin
 * @create 2023/5/26 9:56
 **/
@RemoteService
public class TestRunOfHttpReceiverService {

    private final TestRunService testRuns = new TestRunService(false);

    public static final String TAG = TestRunService.class.getSimpleName();

    @Data
    static class Request {
//    private String connectionsId;


        /**
         * type of test run: source or target
         */
        private String type;

        /**
         * initial_sync
         * cdc
         * initial_sync+cdc
         */
        private String syncType;

        private int timeout;

        /**
         * connections
         */
        private Connections connections;

        private List<Map<String, Object>> params;

        private String version;

        TestRunService.Request asTestRunRequest() {
            TestRunService.Request request = new TestRunService.Request();
            request.setType(type);
            request.setSyncType(syncType);
            request.setTimeout(timeout);
            request.setConnections(connections);
            request.setParams(params);
            request.setVersion(version);
            return request;
        }
    }

    @Data
    @NoArgsConstructor
    static class Response {

        private List<CollectLog.Log> logs;

        /**
         * target
         */
        private long insertedCount;
        private long removedCount;
        private long modifiedCount;

        private String version;

        public Response(List<CollectLog.Log> logs, String version) {
            this.logs = logs;
            this.version = version;
        }

        public void incrementInserted(long value) {
            this.insertedCount = this.insertedCount + value;
        }

        public void incrementModified(long value) {
            this.modifiedCount = this.modifiedCount + value;
        }

        public void incrementRemove(long value) {
            this.removedCount = this.removedCount + value;
        }
    }

    public Response testRun(final Request request) throws Throwable {
        TestRunService.Response result = testRuns.testRun(request == null ? null : request.asTestRunRequest());
        return new Response(result.getLogs(), result.getVersion());
    }
}
