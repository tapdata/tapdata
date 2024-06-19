package com.tapdata.tm.measurementServiceV2.controller;

import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.monitor.controller.MeasureController;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.monitor.service.MeasurementServiceV2Impl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MeasureControllerTest {

    @Test
    void testFindLastMinuteByTaskId(){

        MeasureController measureController = new MeasureController();
        MeasurementServiceV2 measurementServiceV2 = mock(MeasurementServiceV2Impl.class);
        String taskId = "66616f30c7a14d46120ef0b1";
          ReflectionTestUtils.setField(measureController, "measurementServiceV2", measurementServiceV2);
        MeasurementEntity measurementEntity = new MeasurementEntity();
        String taskIdEntity = "66616f30c7a14d46120ef0b2";
        measurementEntity.setId(taskIdEntity);
        when(measurementServiceV2.findLastMinuteByTaskId(taskId)).thenReturn(measurementEntity);
        ResponseMessage<MeasurementEntity> responseMessage = measureController.findLastMinuteByTaskId("{\"where\":{\"taskId\":\"66616f30c7a14d46120ef0b1\"}}");
        Assertions.assertTrue(taskIdEntity.equals(responseMessage.getData().getId()));

    }
}
