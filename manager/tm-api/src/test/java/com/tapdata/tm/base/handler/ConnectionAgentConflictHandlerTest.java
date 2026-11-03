package com.tapdata.tm.base.handler;

import com.tapdata.tm.ds.dto.ConnectionAgentConflictReport;
import com.tapdata.tm.ds.exception.ConnectionAgentConflictException;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import static org.junit.jupiter.api.Assertions.*;

class ConnectionAgentConflictHandlerTest {
    @Test
    void returnsStructuredConflictDataForSaveTimeRechecks() {
        ConnectionAgentConflictReport report = new ConnectionAgentConflictReport();
        report.setHasConflicts(true);
        report.setHiddenCount(1);
        var response = new ExceptionHandler().handlerException(new ConnectionAgentConflictException(report),
                new MockHttpServletRequest(), new MockHttpServletResponse());
        assertEquals(ConnectionAgentConflictException.CODE, response.getCode());
        assertSame(report, response.getData());
    }
}
