package io.tapdata.flow.engine.util;

import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.ErrorEvent;
import io.tapdata.observable.logging.ObsLogger;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TaskDtoUtilTest {

    @Nested
    class UpdateErrorEventTest {
        ClientMongoOperator clientMongoOperator;
        List<ErrorEvent> errorEvents;
        ObsLogger obsLogger;
        String errorMsg;
        ObjectId taskId;
        @BeforeEach
        void init() {
            clientMongoOperator = mock(ClientMongoOperator.class);
            errorEvents = mock(List.class);
            obsLogger = mock(ObsLogger.class);
            errorMsg = "errorMsg";
            taskId = mock(ObjectId.class);
            doNothing().when(clientMongoOperator).insertOne(anyList(), anyString());
            doNothing().when(obsLogger).warn(anyString(), anyString());
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> TaskDtoUtil.updateErrorEvent(clientMongoOperator, errorEvents, taskId, obsLogger, errorMsg));
            verify(clientMongoOperator).insertOne(anyList(), anyString());
            verify(obsLogger, times(0)).warn(anyString(), anyString());
        }

        @Test
        void testException() {
            doAnswer(a -> {
                throw new Exception("xxx");
            }).when(clientMongoOperator).insertOne(anyList(), anyString());
            Assertions.assertDoesNotThrow(() -> TaskDtoUtil.updateErrorEvent(clientMongoOperator, errorEvents, taskId, obsLogger, errorMsg));
            verify(clientMongoOperator).insertOne(anyList(), anyString());
            verify(obsLogger).warn(anyString(), anyString());
        }
    }
}