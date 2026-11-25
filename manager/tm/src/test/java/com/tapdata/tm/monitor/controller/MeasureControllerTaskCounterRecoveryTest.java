package com.tapdata.tm.monitor.controller;

import com.tapdata.tm.monitor.param.MeasurementQueryParam;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MeasureControllerTaskCounterRecoveryTest {
    private MeasureController controller;
    private MeasurementServiceV2 service;
    private MeasurementQueryParam query;

    @BeforeEach
    void setUp() {
        controller = new MeasureController();
        service = mock(MeasurementServiceV2.class);
        query = mock(MeasurementQueryParam.class);
        ReflectionTestUtils.setField(controller, "measurementServiceV2", service);
    }

    @Test
    void recoveryQueryParameterSelectsTheTaskRecoveryServicePath() {
        when(service.getSamples(query, true)).thenReturn("recovered");

        controller.queryV2(query, true);

        verify(service).getSamples(query, true);
        verify(service, never()).getSamples(query);
    }

    @Test
    void absentRecoveryParameterKeepsTheLegacyServicePath() {
        when(service.getSamples(query)).thenReturn("legacy");

        controller.queryV2(query, null);

        verify(service).getSamples(query);
        verify(service, never()).getSamples(query, true);
    }
}
