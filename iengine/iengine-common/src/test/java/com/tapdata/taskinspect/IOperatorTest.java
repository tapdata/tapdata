package com.tapdata.taskinspect;

import com.tapdata.tm.taskinspect.cons.JobTypeEnum;
import com.tapdata.tm.taskinspect.vo.JobReportVo;
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

    private IOperator operator;

    @BeforeEach
    public void setUp() {
        operator = new IOperator() {
        };
    }

    @Test
    public void getConfig_ReturnsNull() {
        assertNull(operator.getConfig("testTaskId"));
    }

    @Test
    public void jobStart_ReturnsNull() {
        assertNull(operator.jobStart("testTaskId", JobTypeEnum.UNKNOWN, new Serializable() {
        }, new LinkedHashMap<>()));
    }

    @Test
    public void postJobStatus_ReturnsFalse() {
        assertFalse(operator.postJobStatus("testJobId", new JobReportVo()));
    }

    @Test
    public void params_ReturnsCorrectMap() {
        LinkedHashMap<String, Serializable> params = operator.params("key1", "value1");
        assertEquals(1, params.size());
        assertEquals("value1", params.get("key1"));
    }

    @Test
    public void params_AddsToExistingMap() {
        LinkedHashMap<String, Serializable> existingParams = new LinkedHashMap<>();
        existingParams.put("key1", "value1");
        LinkedHashMap<String, Serializable> params = operator.params(existingParams, "key2", "value2");
        assertEquals(2, params.size());
        assertEquals("value1", params.get("key1"));
        assertEquals("value2", params.get("key2"));
    }
}
