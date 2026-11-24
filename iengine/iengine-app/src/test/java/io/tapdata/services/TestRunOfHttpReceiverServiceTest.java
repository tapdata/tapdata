package io.tapdata.services;

import com.tapdata.entity.Connections;
import io.tapdata.flow.engine.V2.log.CollectLog;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TestRunOfHttpReceiverServiceTest {
    @Test
    void forwardsTheSavedDatasourceAndOptionsToASourceOnlyRun() throws Throwable {
        Connections datasource = new Connections();
        datasource.setId("0123456789abcdef01234567");
        TestRunOfHttpReceiverService.Request request = new TestRunOfHttpReceiverService.Request();
        request.setConnections(datasource);
        request.setType("source");
        request.setSyncType("initial_sync+cdc");
        request.setTimeout(30);
        request.setVersion("1");
        request.setParams(List.of(Map.of("table", "example")));

        List<CollectLog.Log> logs = List.of(new CollectLog.Log("INFO", "complete", 1L));
        try (MockedConstruction<TestRunService> delegate = mockConstruction(TestRunService.class, (service, creation) -> {
            assertEquals(List.of(false), creation.arguments());
            when(service.testRun(any())).thenReturn(new TestRunService.Response(logs, "1"));
        })) {
            TestRunOfHttpReceiverService.Response response = new TestRunOfHttpReceiverService().testRun(request);
            ArgumentCaptor<TestRunService.Request> options = ArgumentCaptor.forClass(TestRunService.Request.class);
            verify(delegate.constructed().get(0)).testRun(options.capture());
            assertSame(datasource, options.getValue().getConnections());
            assertEquals(request.getType(), options.getValue().getType());
            assertEquals(request.getSyncType(), options.getValue().getSyncType());
            assertEquals(request.getTimeout(), options.getValue().getTimeout());
            assertEquals(request.getParams(), options.getValue().getParams());
            assertEquals(request.getVersion(), options.getValue().getVersion());
            assertEquals(logs, response.getLogs());
            assertEquals("1", response.getVersion());
            assertEquals(0, response.getInsertedCount());
            assertEquals(0, response.getModifiedCount());
            assertEquals(0, response.getRemovedCount());
        }
    }

    @Test
    void nullRequestReturnsValidationLogsInTheReceiverResponse() throws Throwable {
        TestRunOfHttpReceiverService.Response response = new TestRunOfHttpReceiverService().testRun(null);
        assertEquals("", response.getVersion());
        assertEquals(1, response.getLogs().size());
        assertEquals("ERROR", response.getLogs().get(0).getLevel());
    }
}
