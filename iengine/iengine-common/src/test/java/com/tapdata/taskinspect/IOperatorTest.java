package com.tapdata.taskinspect;

import com.tapdata.tm.taskinspect.cons.JobType;
import com.tapdata.tm.taskinspect.vo.JobReportVo;
import com.tapdata.tm.taskinspect.vo.ResultsRecoverVo;
import com.tapdata.tm.taskinspect.vo.ResultsReportVo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/4/12 15:19 Create
 */
class IOperatorTest {

    private IOperator iOperator;

    @BeforeEach
    public void setUp() {
        iOperator = new IOperator() {
        };
    }

    @Test
    public void getConfig_ReturnsNull() {
        assertNull(iOperator.getConfig("testTaskId"));
    }

    @Test
    public void jobStart_ReturnsNull() {
        assertNull(iOperator.jobStart("testTaskId", JobType.UNKNOWN, new Serializable() {
        }, new LinkedHashMap<>()));
    }

    @Test
    public void postJobStatus_ReturnsFalse() {
        assertFalse(iOperator.postJobStatus("testJobId", new JobReportVo()));
    }

    @Test
    public void postResults_NoException() {
        iOperator.postResults("testJobId", new ResultsReportVo());
    }

    @Test
    public void postRecover_NoException() {
        iOperator.postRecover("testJobId", new ResultsRecoverVo());
    }

    @Test
    public void params_ReturnsCorrectMap() {
        LinkedHashMap<String, Serializable> params = iOperator.params("key1", "value1");
        assertEquals(1, params.size());
        assertEquals("value1", params.get("key1"));
    }

    @Test
    public void params_AddsToExistingMap() {
        LinkedHashMap<String, Serializable> existingParams = new LinkedHashMap<>();
        existingParams.put("key1", "value1");
        LinkedHashMap<String, Serializable> params = iOperator.params(existingParams, "key2", "value2");
        assertEquals(2, params.size());
        assertEquals("value1", params.get("key1"));
        assertEquals("value2", params.get("key2"));
    }
}
